import fs from 'fs';
import path from 'path';

export const cleanTmpDirectory = async () => {
    try {
        const files = await fs.readdirSync('/tmp');
        files.forEach((file) => {
            const filePath = path.join('/tmp', file);
            fs.unlinkSync(filePath);
        });
    } catch (error) {
        throw error;
    }
};

export const generateTmpFilePath = (extension) => {
    try {
        const hash = getRandomString(10);
        return `/tmp/doc-${hash}.${extension}`;
    } catch (error) {
        throw error;
    }
};

const getRandomString = (len) => {
    try {
        const charset =
            '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        let result = '';
        for (let i = len; i > 0; --i) {
            result += charset[Math.floor(Math.random() * charset.length)];
        }
        return result;
    } catch (error) {
        throw error;
    }
};
