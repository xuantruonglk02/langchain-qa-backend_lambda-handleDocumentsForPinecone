import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { PineconeClient } from '@pinecone-database/pinecone';
import fs from 'fs';
import { DocxLoader } from 'langchain/document_loaders/fs/docx';
import { PDFLoader } from 'langchain/document_loaders/fs/pdf';
import { OpenAIEmbeddings } from 'langchain/embeddings/openai';
import { RecursiveCharacterTextSplitter } from 'langchain/text_splitter';
import { PineconeStore } from 'langchain/vectorstores/pinecone';
import mongoose from 'mongoose';
import { cleanTmpDirectory, generateTmpFilePath } from './helpers.mjs';

const DocumentExtension = {
    DOCX: 'docx',
    PDF: 'pdf',
};

export const handler = async (event) => {
    try {
        const bucketRegion = event.Records[0].awsRegion;
        const bucketName = event.Records[0].s3.bucket.name;
        const fileKey = event.Records[0].s3.object.key;
        const fileExtension = fileKey.split('.').at(-1);

        // download file
        // get file information in db
        const [fileAsBuffer, file] = await Promise.all([
            downloadFile(bucketRegion, bucketName, fileKey),
            getFileDetailFromDB(fileKey),
        ]);
        const fileId = file._id.toString();
        const userId = file.createdBy.toString();

        // save file to tmp folder
        const tmpFilePath = await saveFile(fileAsBuffer, fileExtension);

        // load and split file
        const splittedDoc = await splitFile(tmpFilePath, fileExtension);
        // add file information into metadata
        splittedDoc.forEach((doc) => {
            doc.metadata.fileId = fileId;
            doc.metadata.userId = userId;
        });

        // add document to Pinecone
        await addDocumentToPinecone(splittedDoc);

        cleanTmpDirectory();

        return {
            statusCode: 200,
            body: {
                message: 'Handle document successfully',
                fileKey,
            },
        };
    } catch (error) {
        cleanTmpDirectory();
        return {
            statusCode: 500,
            body: {
                error,
                event,
            },
        };
    }
};

async function downloadFile(bucketRegion, bucketName, fileKey) {
    try {
        const s3Client = new S3Client({
            region: bucketRegion,
        });
        const getObjectCommand = new GetObjectCommand({
            Bucket: bucketName,
            Key: fileKey,
        });
        const getResponse = await s3Client.send(getObjectCommand);
        const fileAsBuffer = await getResponse.Body.transformToByteArray();
        return fileAsBuffer;
    } catch (error) {
        throw error;
    }
}

async function getFileDetailFromDB(fileKey) {
    try {
        const connection = mongoose.createConnection(
            process.env.MONGODB_CONNECTION_STRING
        );
        const FileModel = connection.model('File', {}, 'files');

        const file = await FileModel.findOne({
            key: fileKey,
            deletedAt: {
                $exists: true,
                $eq: null,
            },
        }).lean();
        return file;
    } catch (error) {
        throw error;
    }
}

async function saveFile(fileAsBuffer, fileExtension) {
    try {
        const tmpFilePath = generateTmpFilePath(fileExtension);
        await fs.writeFileSync(tmpFilePath, fileAsBuffer, 'base64');
        return tmpFilePath;
    } catch (error) {
        throw error;
    }
}

async function splitFile(tmpFilePath, fileExtension) {
    try {
        const loader = getLoaderOfDocument(tmpFilePath, fileExtension);
        const document = await loader.load();
        const splittedDoc = await splitDocument(document);
        return splittedDoc;
    } catch (error) {
        throw error;
    }
}

function getLoaderOfDocument(filePath, fileExtension) {
    try {
        switch (fileExtension) {
            case DocumentExtension.DOCX:
                return new DocxLoader(filePath);
            case DocumentExtension.PDF:
                return new PDFLoader(filePath);
            default:
                return null;
        }
    } catch (error) {
        throw error;
    }
}

async function splitDocument(doc) {
    try {
        const splitter = new RecursiveCharacterTextSplitter({
            chunkSize: 1000,
            chunkOverlap: 200,
            separators: ['\n\n', '\n', ' ', ''],
        });
        const splittedDoc = await splitter.splitDocuments(doc);
        return splittedDoc;
    } catch (error) {
        throw error;
    }
}

async function addDocumentToPinecone(splittedDoc) {
    try {
        const openAIEmbedding = new OpenAIEmbeddings({
            openAIApiKey: process.env.OPENAI_API_KEY,
        });
        const pineconeClient = new PineconeClient();
        await pineconeClient.init({
            apiKey: process.env.PINECONE_API_KEY,
            environment: process.env.PINECONE_ENVIRONMENT,
        });
        const pineconeIndex = pineconeClient.Index(process.env.PINECONE_INDEX);
        await PineconeStore.fromDocuments(splittedDoc, openAIEmbedding, {
            pineconeIndex,
        });
    } catch (error) {
        throw error;
    }
}
