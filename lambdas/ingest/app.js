const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");

const sqs = new SQSClient();
const QUEUE_URL = process.env.QUEUE_URL;

exports.handler = async (event) => {
    console.log("Received event:", JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        const bucketName = record.s3.bucket.name;
        const objectKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));
        const etag = record.s3.object.eTag;

        // Validate extension
        const lowerKey = objectKey.toLowerCase();
        if (!lowerKey.endsWith('.jpg') && !lowerKey.endsWith('.jpeg') && !lowerKey.endsWith('.png')) {
            console.log(`Skipping non-image file: ${objectKey}`);
            continue;
        }

        const messageBody = {
            bucket: bucketName,
            key: objectKey,
            etag: etag
        };

        try {
            await sqs.send(new SendMessageCommand({
                QueueUrl: QUEUE_URL,
                MessageBody: JSON.stringify(messageBody)
            }));
            console.log(`Sent message to queue for ${objectKey}`);
        } catch (error) {
            console.error("Error sending to queue:", error);
            throw error;
        }
    }

    return { statusCode: 200, body: "Ingestion complete" };
};
