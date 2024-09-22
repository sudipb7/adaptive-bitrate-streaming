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
  ACCESS_KEY_ID,
  SECRET_ACCESS_KEY,
  REGION,
  QUEUE_URL,
  PROD_BUCKET,
  TASK_DEFINITION_ARN,
  CLUSTER_ARN,
  SUBNETS,
  SECURITY_GROUP,
} = process.env;

const client = new SQSClient({
  region: REGION,
  credentials: {
    accessKeyId: ACCESS_KEY_ID!,
    secretAccessKey: SECRET_ACCESS_KEY!,
  },
});

const ecsClient = new ECSClient({
  region: REGION,
  credentials: {
    accessKeyId: ACCESS_KEY_ID!,
    secretAccessKey: SECRET_ACCESS_KEY!,
  },
});

async function main() {
  const command = new ReceiveMessageCommand({
    QueueUrl: QUEUE_URL,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 20,
  });

  while (true) {
    const { Messages } = await client.send(command);
    if (!Messages) {
      console.log("No messages found");
      continue;
    }

    try {
      for (const message of Messages) {
        const { Body, MessageId } = message;
        console.log("Message Revieved: ", { MessageId, Body });

        if (!Body) {
          console.log("No body found in the message");
          continue;
        }

        // Validate & Parse the event
        const event = JSON.parse(Body) as S3Event;

        // Ignore the test event
        if ("Service" in event && "Event" in event) {
          if (event.Event === "s3:TestEvent") {
            console.log("Test event found");
            await client.send(
              new DeleteMessageCommand({
                QueueUrl: QUEUE_URL,
                ReceiptHandle: message.ReceiptHandle,
              })
            );
            continue;
          }
        }

        for (const record of event.Records) {
          const { s3 } = record;
          const {
            bucket,
            object: { key },
          } = s3;

          // Spin the docker container
          const runTaskCommand = new RunTaskCommand({
            taskDefinition: TASK_DEFINITION_ARN, // Task Definition ARN
            cluster: CLUSTER_ARN, // Cluster ARN
            launchType: "FARGATE",
            networkConfiguration: {
              awsvpcConfiguration: {
                assignPublicIp: "ENABLED",
                securityGroups: SECURITY_GROUP ? [SECURITY_GROUP] : [], // Security Group IDs
                subnets: SUBNETS ? SUBNETS.split(",") : [], // Subnet IDs
              },
            },
            overrides: {
              containerOverrides: [
                {
                  name: "video-transcoder",
                  environment: [
                    { name: "KEY", value: key },
                    { name: "REGION", value: REGION },
                    { name: "BUCKET", value: bucket.name },
                    { name: "PROD_BUCKET", value: PROD_BUCKET },
                    { name: "ACCESS_KEY_ID", value: ACCESS_KEY_ID },
                    { name: "SECRET_ACCESS_KEY", value: SECRET_ACCESS_KEY },
                  ],
                },
              ],
            },
          });

          await ecsClient.send(runTaskCommand);

          // Delete the message
          await client.send(
            new DeleteMessageCommand({
              QueueUrl: QUEUE_URL,
              ReceiptHandle: message.ReceiptHandle,
            })
          );
        }
      }
    } catch (error) {
      console.log(error);
    }
  }
}

main();
