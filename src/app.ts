import "dotenv/config";
import express from "express";
import {
    S3Client,
    ListBucketsCommand,
    HeadObjectCommand,
    GetObjectCommand,
    ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import tar from "tar-stream";
import zlib from "zlib";
import stream from "stream";
import { promisify } from "util";
import fs from "fs";
import { pipeline } from "stream/promises";
import { S3ReadStream } from "s3-readstream";

const client = new S3Client({
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
    region: "eu-west-1",
});

const bucket = "bucket2828";

async function getKeysInBucket() {
    const command = new ListObjectsV2Command({ Bucket: bucket });
    const response = await client.send(command);
    return response.Contents?.map((content) => content.Key);
}

async function main() {
    const pack = tar.pack();
    const gzip = zlib.createGzip();
    const tarGzFilePath = "./dl/archive.tar.gz";

    if (!fs.existsSync("./dl")) {
        fs.mkdirSync("./dl");
    }

    const keys = await getKeysInBucket();
    const tarGzWriteStream = fs.createWriteStream(tarGzFilePath);

    for (const key of keys) {
        const s3Params = {
            Bucket: bucket,
            Key: key,
        };

        const headObjectCommand = new HeadObjectCommand(s3Params);
        const headObject = await client.send(headObjectCommand);

        const options = {
            s3: client,
            command: new GetObjectCommand(s3Params),
            maxLength: headObject.ContentLength,
            byteRange: 1024 * 1024 * 10,
        };

        const file = new S3ReadStream(options);

        const entry = pack.entry(
            { name: key, size: headObject.ContentLength },
            function (err) {
                if (err) {
                    console.error("Error adding entry to tar pack:", err);
                    return;
                }
                entry.end();
            }
        );

        file.pipe(entry);
    }

    pack.finalize();

    await pipeline(pack, gzip, tarGzWriteStream);
}

main().catch(console.error);

// setInterval(() => {
//     const used = process.memoryUsage().heapUsed / 1024 / 1024;
//     console.log(`The script uses approximately ${used} MB`);
// }, 1000);
