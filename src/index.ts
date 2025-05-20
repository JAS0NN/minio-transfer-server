#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
    CallToolRequestSchema,
    ErrorCode,
    ListToolsRequestSchema,
    McpError,
} from '@modelcontextprotocol/sdk/types.js';
import * as Minio from 'minio';
import archiver from 'archiver';
import fs from 'fs';
import path from 'path';
import tmp from 'tmp-promise';
import { Readable } from 'stream';

// MinIO Configuration from Environment Variables
const MINIO_ENDPOINT = process.env.MCP_MINIO_ENDPOINT;
const MINIO_PORT = process.env.MCP_MINIO_PORT ? parseInt(process.env.MCP_MINIO_PORT, 10) : undefined;
const MINIO_ACCESS_KEY = process.env.MCP_MINIO_ACCESS_KEY;
const MINIO_SECRET_KEY = process.env.MCP_MINIO_SECRET_KEY;
const MINIO_USE_SSL = process.env.MCP_MINIO_SECURE === 'true';

if (!MINIO_ENDPOINT || !MINIO_ACCESS_KEY || !MINIO_SECRET_KEY) {
    console.error('MinIO environment variables (MCP_MINIO_ENDPOINT, MCP_MINIO_ACCESS_KEY, MCP_MINIO_SECRET_KEY) are required.');
    process.exit(1);
}

const minioClient = new Minio.Client({
    endPoint: MINIO_ENDPOINT,
    port: MINIO_PORT,
    useSSL: MINIO_USE_SSL,
    accessKey: MINIO_ACCESS_KEY,
    secretKey: MINIO_SECRET_KEY,
});

// Helper function to check if path is directory
const isDirectory = async (sourcePath: string): Promise<boolean> => {
    try {
        const stats = await fs.promises.stat(sourcePath);
        return stats.isDirectory();
    } catch (error) {
        console.error(`Error checking path ${sourcePath}:`, error);
        throw new McpError(ErrorCode.InvalidParams, `Error accessing path: ${sourcePath}. ${error instanceof Error ? error.message : String(error)}`);
    }
};

// Helper function to create a .tar.gz archive
const createArchive = async (sourcePath: string, isDir: boolean): Promise<string> => {
    const { path: tmpPath, cleanup } = await tmp.file({ postfix: '.tar.gz' });
    const output = fs.createWriteStream(tmpPath);
    const archive = archiver('tar', {
        gzip: true,
        gzipOptions: {
            level: 1,
        },
    });

    return new Promise((resolve, reject) => {
        output.on('close', () => {
            console.log(`Archive created: ${tmpPath}, ${archive.pointer()} total bytes`);
            resolve(tmpPath);
        });
        archive.on('error', (err) => {
            console.error('Archive creation error:', err);
            cleanup();
            reject(new McpError(ErrorCode.InternalError, `Failed to create archive: ${err.message}`));
        });

        archive.pipe(output);
        if (isDir) {
            console.log(`Archiving directory: ${sourcePath}`);
            archive.directory(sourcePath, false);
        } else {
            console.log(`Archiving file: ${sourcePath}`);
            archive.file(sourcePath, { name: path.basename(sourcePath) });
        }
        archive.finalize();
    });
};


class MinioTransferServer {
    private server: Server;

    constructor() {
        this.server = new Server(
            {
                name: 'minio-transfer-server',
                version: '0.1.0',
                description: 'A server to transfer files using MinIO storage via the Model Context Protocol (MCP).'
            },
            {
                capabilities: {
                    tools: {},
                },
            }
        );

        this.setupToolHandlers();

        this.server.onerror = (error) => {
            console.error('[MCP Error]', error);
            // Ensure McpError is propagated correctly
            if (error instanceof McpError) {
                // Potentially re-throw or handle specific MCP errors if needed
            }
        };
        process.on('SIGINT', async () => {
            console.log('Shutting down MinIO Transfer Server...');
            await this.server.close();
            process.exit(0);
        });
    }

    private setupToolHandlers() {
        this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
            tools: [
                {
                    name: 'upload_to_minio',
                    description: 'Compresses and uploads a local file or directory to MinIO, returning a presigned URL and object name.',
                    inputSchema: {
                        type: 'object',
                        properties: {
                            local_path: { type: 'string', description: 'Path to the local file or directory to upload.' },
                            bucket_name: { type: 'string', description: 'MinIO bucket name.', default: 'ic-transfer' },
                            expires_minutes: { type: 'integer', description: 'Presigned URL validity in minutes.', default: 15 },
                        },
                        required: ['local_path'],
                    },
                },
                {
                    name: 'download_from_minio_by_url',
                    description: 'Downloads a file from MinIO using a presigned URL to the specified local path.',
                    inputSchema: {
                        type: 'object',
                        properties: {
                            presigned_url: { type: 'string', description: 'Presigned URL provided by MinIO.' },
                            local_download_path: { type: 'string', description: 'Local path (including filename) to save the downloaded file.' },
                        },
                        required: ['presigned_url', 'local_download_path'],
                    },
                },
            ],
        }));

        this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
            console.log(`Received tool call for: ${request.params.name}`);
            try {
                if (request.params.name === 'upload_to_minio') {
                    return this.handleUploadToMinio(request.params.arguments);
                } else if (request.params.name === 'download_from_minio_by_url') {
                    return this.handleDownloadFromMinioByUrl(request.params.arguments);
                } else {
                    console.error(`Unknown tool: ${request.params.name}`);
                    throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${request.params.name}`);
                }
            } catch (error) {
                console.error(`Error handling tool ${request.params.name}:`, error);
                const message = error instanceof Error ? error.message : String(error);
                // Ensure the error is an McpError or wrap it
                if (error instanceof McpError) {
                    throw error;
                }
                throw new McpError(ErrorCode.InternalError, `Error processing ${request.params.name}: ${message}`);
            }
        });
    }

    private async handleUploadToMinio(args: any) {
        const { local_path, bucket_name = 'ic-transfer', expires_minutes = 15 } = args;

        if (!local_path || typeof local_path !== 'string') {
            throw new McpError(ErrorCode.InvalidParams, 'local_path (string) is required.');
        }
        if (typeof bucket_name !== 'string') {
            throw new McpError(ErrorCode.InvalidParams, 'bucket_name must be a string.');
        }
        if (typeof expires_minutes !== 'number' || !Number.isInteger(expires_minutes) || expires_minutes <= 0) {
            throw new McpError(ErrorCode.InvalidParams, 'expires_minutes must be a positive integer.');
        }

        console.log(`Upload request: local_path=${local_path}, bucket_name=${bucket_name}, expires_minutes=${expires_minutes}`);

        let tmpArchivePath: string | undefined;
        let objectName: string;

        try {
            const isDir = await isDirectory(local_path);
            const sourceName = path.basename(local_path);
            objectName = isDir ? `${sourceName}-${Date.now()}.tar.gz` : `${sourceName}-${Date.now()}${path.extname(local_path) || '.bin'}`;
            
            if (isDir) {
                 console.log(`Path is a directory: ${local_path}. Archiving...`);
                 tmpArchivePath = await createArchive(local_path, true);
                 objectName = `${path.basename(local_path)}-${Date.now()}.tar.gz`; // Ensure .tar.gz for directories
            } else {
                // For single files, if it's already a common archive type, consider uploading directly
                // For simplicity here, we'll archive single files too, but this could be optimized.
                // If not archiving single files, objectName would be path.basename(local_path)
                // and the upload stream would be fs.createReadStream(local_path)
                console.log(`Path is a file: ${local_path}. Archiving...`);
                tmpArchivePath = await createArchive(local_path, false);
                // Use a unique name for the archived single file to avoid collisions if original name is generic
                objectName = `${path.parse(local_path).name}-${Date.now()}${path.extname(tmpArchivePath)}`;
            }

            const fileStream = fs.createReadStream(tmpArchivePath);
            const stats = await fs.promises.stat(tmpArchivePath);

            console.log(`Checking if bucket '${bucket_name}' exists...`);
            const bucketExists = await minioClient.bucketExists(bucket_name);
            if (!bucketExists) {
                console.log(`Bucket '${bucket_name}' does not exist. Creating...`);
                await minioClient.makeBucket(bucket_name, MINIO_ENDPOINT!.startsWith('s3.amazonaws.com') ? '' : 'us-east-1'); // Region is required for some MinIO setups but not for AWS S3 itself via endpoint
                console.log(`Bucket '${bucket_name}' created.`);
            } else {
                console.log(`Bucket '${bucket_name}' already exists.`);
            }
            
            console.log(`Uploading '${objectName}' to bucket '${bucket_name}' from ${tmpArchivePath}`);
            await minioClient.putObject(bucket_name, objectName, fileStream, stats.size);
            console.log(`Successfully uploaded '${objectName}' to bucket '${bucket_name}'.`);

            const presignedUrl = await minioClient.presignedGetObject(bucket_name, objectName, expires_minutes * 60);
            console.log(`Generated presigned URL: ${presignedUrl}`);

            return {
                content: [
                    {
                        type: 'text',
                        text: JSON.stringify({
                            presigned_url: presignedUrl,
                            object_name: objectName,
                            status: 'success',
                        }),
                    },
                ],
            };
        } catch (error) {
            console.error('Upload to MinIO failed:', error);
            const message = error instanceof Error ? error.message : String(error);
            // Propagate McpError directly
            if (error instanceof McpError) {
                 return {
                    content: [{ type: 'text', text: JSON.stringify({ status: 'error', message: error.message, code: error.code }) }],
                    isError: true,
                };
            }
            return {
                content: [{ type: 'text', text: JSON.stringify({ status: 'error', message: `Upload failed: ${message}` }) }],
                isError: true,
            };
        } finally {
            if (tmpArchivePath) {
                console.log(`Cleaning up temporary archive: ${tmpArchivePath}`);
                try {
                    await fs.promises.unlink(tmpArchivePath);
                    console.log(`Successfully deleted temporary archive: ${tmpArchivePath}`);
                } catch (cleanupError) {
                    console.error(`Failed to delete temporary archive ${tmpArchivePath}:`, cleanupError);
                }
            }
        }
    }

    private async handleDownloadFromMinioByUrl(args: any) {
        const { presigned_url, local_download_path } = args;

        if (!presigned_url || typeof presigned_url !== 'string') {
            throw new McpError(ErrorCode.InvalidParams, 'presigned_url (string) is required.');
        }
        if (!local_download_path || typeof local_download_path !== 'string') {
            throw new McpError(ErrorCode.InvalidParams, 'local_download_path (string) is required.');
        }
        
        console.log(`Download request: presigned_url=${presigned_url}, local_download_path=${local_download_path}`);

        try {
            // Ensure the directory for the download path exists
            const downloadDir = path.dirname(local_download_path);
            await fs.promises.mkdir(downloadDir, { recursive: true });
            console.log(`Ensured directory exists: ${downloadDir}`);

            // MinIO SDK's getObject using a presigned URL is not straightforward.
            // We'll use a direct HTTP GET request to the presigned URL.
            // This requires 'axios' or a similar HTTP client. Let's add it or use Node's http/https.
            // For simplicity and to avoid adding another dependency if not already present in a typical MCP server template,
            // we'll use Node's built-in http/https module.

            const protocol = presigned_url.startsWith('https:') ? 'https' : 'http';
            const { default: httpLib } = await import(protocol);

            await new Promise((resolve, reject) => {
                const file = fs.createWriteStream(local_download_path);
                const request = httpLib.get(presigned_url, (response: any) => { // Add 'any' type for response due to dynamic import
                    if (response.statusCode !== 200) {
                        let errorData = '';
                        response.on('data', (chunk: any) => errorData += chunk); // Add 'any' type for chunk
                        response.on('end', () => {
                             console.error(`Download failed with status ${response.statusCode}: ${errorData}`);
                             file.close();
                             fs.unlink(local_download_path, () => {}); // Attempt to delete partial file
                             reject(new McpError(ErrorCode.InternalError, `Download failed: Server responded with status ${response.statusCode}. ${errorData}`));
                        });
                        return;
                    }
                    response.pipe(file);
                    file.on('finish', () => {
                        file.close();
                        console.log(`File downloaded successfully to ${local_download_path}`);
                        resolve(local_download_path);
                    });
                });
                request.on('error', (err: Error) => {
                    console.error('Error during download HTTP request:', err);
                    fs.unlink(local_download_path, () => {}); // Attempt to delete partial file
                    reject(new McpError(ErrorCode.InternalError, `Download request failed: ${err.message}`));
                });
            });

            return {
                content: [
                    {
                        type: 'text',
                        text: JSON.stringify({
                            status: 'success',
                            message: `File downloaded successfully to ${local_download_path}`,
                            local_file_path: local_download_path,
                        }),
                    },
                ],
            };
        } catch (error) {
            console.error('Download from MinIO failed:', error);
            const message = error instanceof Error ? error.message : String(error);
             if (error instanceof McpError) {
                 return {
                    content: [{ type: 'text', text: JSON.stringify({ status: 'error', message: error.message, code: error.code }) }],
                    isError: true,
                };
            }
            return {
                content: [{ type: 'text', text: JSON.stringify({ status: 'error', message: `Download failed: ${message}` }) }],
                isError: true,
            };
        }
    }

    async run() {
        const transport = new StdioServerTransport();
        await this.server.connect(transport);
        // Removed console.error logs here as they might be misinterpreted as errors by the client.
        // The server being connected is an implicit indication of it running.
        // MinIO config is logged at startup if needed, or can be inferred from env.
    }
}

const server = new MinioTransferServer();
server.run().catch(error => {
    // Catch unhandled promise rejections from run()
    console.error('Failed to start MinIO Transfer Server:', error);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  // Optionally exit or log more detailed diagnostics
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  // Graceful shutdown logic might be needed here if the server is running
  process.exit(1); // Mandatory exit after uncaught exception
});
