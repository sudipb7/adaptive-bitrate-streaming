import path from "node:path";
import dotenv from "dotenv";
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";
import type { S3Event } from "aws-lambda";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";

dotenv.config({ path: path.resolve(__dirname, "../.env") });

const {
  AWS_SECRET_ACCESS_KEY,
  AWS_ACCESS_KEY_ID,
  AWS_DEFAULT_REGION,
  QUEUE_URL,
  PROD_BUCKET,
  TASK_DEFINITION_ARN,
  CLUSTER_ARN,
  SUBNETS,
  SECURITY_GROUP,
} = process.env;

const ACCEPTABLE_FORMATS = [
  ".MP4", // Recommended format
  ".MOV", // Commonly used
  ".AVI", // Widely used
  ".WMV", // Windows format
  ".FLV", // Flash videos
  ".WEBM", // Web use
];

const createSQSClient = () =>
  new SQSClient({
    region: AWS_DEFAULT_REGION,
    credentials: {
      accessKeyId: AWS_ACCESS_KEY_ID!,
      secretAccessKey: AWS_SECRET_ACCESS_KEY!,
    },
  });

const createECSClient = () =>
  new ECSClient({
    region: AWS_DEFAULT_REGION,
    credentials: {
      accessKeyId: AWS_ACCESS_KEY_ID!,
      secretAccessKey: AWS_SECRET_ACCESS_KEY!,
    },
  });

const sqsClient = createSQSClient();
const ecsClient = createECSClient();

async function deleteMessageFromQueue(receiptHandle: string) {
  const deleteMessageCommand = new DeleteMessageCommand({
    QueueUrl: QUEUE_URL,
    ReceiptHandle: receiptHandle,
  });

  try {
    await sqsClient.send(deleteMessageCommand);
    console.log("Message deleted from the queue:", receiptHandle);
  } catch (error) {
    console.error("Error deleting message from the queue:", error);
  }
}

async function processMessage(message: any) {
  const { Body, MessageId, ReceiptHandle } = message;
  console.log("Message Received: ", { MessageId, Body });

  if (!Body) {
    console.error("No body found in the message");
    return;
  }

  let event: S3Event;
  try {
    event = JSON.parse(Body) as S3Event;
  } catch (error) {
    console.error("Error parsing S3 event:", error);
    return;
  }

  if (
    "Service" in event &&
    "Event" in event &&
    event.Event === "s3:TestEvent"
  ) {
    await deleteMessageFromQueue(ReceiptHandle!);
    return;
  }

  for (const record of event.Records) {
    const { s3 } = record;
    const {
      bucket,
      object: { key },
    } = s3;

    if (!bucket?.name) {
      console.error("Bucket name missing in the S3 event.");
      continue;
    }

    if (!key) {
      console.error("S3 object key missing.");
      continue;
    }

    const fileExtension = path.extname(key).toUpperCase();
    if (!ACCEPTABLE_FORMATS.includes(fileExtension)) {
      console.error(
        `Invalid video format: ${fileExtension}. Accepted formats are: ${ACCEPTABLE_FORMATS.join(
          ", "
        )}`
      );
      continue;
    }

    console.log(
      `File ${key} in bucket ${bucket.name} is valid for processing.`
    );

    const runTaskCommand = new RunTaskCommand({
      taskDefinition: TASK_DEFINITION_ARN,
      cluster: CLUSTER_ARN,
      launchType: "FARGATE",
      networkConfiguration: {
        awsvpcConfiguration: {
          assignPublicIp: "ENABLED",
          securityGroups: SECURITY_GROUP ? [SECURITY_GROUP] : [],
          subnets: SUBNETS ? SUBNETS.split(",") : [],
        },
      },
      overrides: {
        containerOverrides: [
          {
            name: "video-transcoder",
            environment: [
              { name: "KEY", value: key },
              { name: "BUCKET", value: bucket.name },
              { name: "PROD_BUCKET", value: PROD_BUCKET },
              { name: "AWS_DEFAULT_REGION", value: AWS_DEFAULT_REGION },
              { name: "AWS_ACCESS_KEY_ID", value: AWS_ACCESS_KEY_ID },
              { name: "AWS_SECRET_ACCESS_KEY", value: AWS_SECRET_ACCESS_KEY },
            ],
          },
        ],
      },
    });

    try {
      await ecsClient.send(runTaskCommand);
      await deleteMessageFromQueue(ReceiptHandle!);
      console.log(`Message ${MessageId} processed and deleted.`);
    } catch (error) {
      console.error("Error running ECS task or deleting SQS message:", error);
    }
  }
}

async function main() {
  const command = new ReceiveMessageCommand({
    QueueUrl: QUEUE_URL,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 20,
  });

  while (true) {
    try {
      const { Messages } = await sqsClient.send(command);
      if (!Messages) {
        console.log("No messages found");
        continue;
      }

      for (const message of Messages) {
        await processMessage(message);
      }
    } catch (error) {
      console.error("Error receiving SQS messages:", error);
    }
  }
}

main().catch((error) => {
  console.error("Error in main loop:", error);
});
