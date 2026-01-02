const { S3Client, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { imageSize } = require('image-size');
const path = require('path');

const s3 = new S3Client();

// Helper to read stream to buffer
const streamToBuffer = (stream) =>
    new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks)));
    });

exports.handler = async (event) => {
    console.log("Received event:", JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            const body = JSON.parse(record.body);
            const bucket = body.bucket;
            const key = body.key;

            if (!key.startsWith("incoming/")) {
                console.log(`Ignoring key not in incoming/: ${key}`);
                continue;
            }

            // Download image
            const getObjectParams = {
                Bucket: bucket,
                Key: key
            };
            const response = await s3.send(new GetObjectCommand(getObjectParams));
            const fileBuffer = await streamToBuffer(response.Body);
            const fileSize = response.ContentLength;

            // Extract Metadata
            let dimensions;
            try {
                dimensions = imageSize(fileBuffer);
            } catch (err) {
                console.error(`Failed to process image ${key}:`, err);
                continue;
            }

            const metadata = {
                source_bucket: bucket,
                source_key: key,
                width: dimensions.width,
                height: dimensions.height,
                file_size_bytes: fileSize,
                format: dimensions.type.toUpperCase()
            };

            // Write to S3
            const filename = path.basename(key);
            const metadataKey = `metadata/${filename}.json`;

            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: metadataKey,
                Body: JSON.stringify(metadata),
                ContentType: 'application/json'
            }));

            console.log(`Wrote metadata to ${metadataKey}`);

        } catch (error) {
            console.error("Error processing record:", error);
            throw error; // Trigger SQS retry
        }
    }

    return { statusCode: 200, body: "Processing complete" };
};
