const fs = require("node:fs");
const path = require("node:path");
const ffmpeg = require("fluent-ffmpeg");
const fsPromises = require("node:fs/promises");
const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");

const { ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION, BUCKET, KEY, PROD_BUCKET } =
  process.env;

const RESOLUTIONS = [
  { name: "360p", width: 480, height: 360 },
  { name: "480p", width: 640, height: 480 },
  { name: "720p", width: 1280, height: 720 },
];

const s3Client = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
  },
});

async function main() {
  // Download the video
  const command = new GetObjectCommand({
    Bucket: BUCKET,
    Key: KEY,
  });

  const result = await s3Client.send(command);

  const originalFilePath = `original-video.mp4`;
  await fsPromises.writeFile(originalFilePath, result.Body);

  const originalVideoPath = path.resolve(originalFilePath);

  // Start the Transcoder
  const promises = RESOLUTIONS.map((resolution) => {
    const output = `video-${resolution.name}.mp4`;

    return new Promise((resolve) => {
      ffmpeg(originalVideoPath)
        .output(output)
        .withVideoCodec("libx264")
        .withAudioCodec("aac")
        .withSize(`${resolution.width}x${resolution.height}`)
        .on("end", async () => {
          // Upload the video
          const putCommand = new PutObjectCommand({
            Bucket: PROD_BUCKET,
            Key: output,
            Body: fs.createReadStream(path.resolve(output)),
          });

          await s3Client.send(putCommand);
          console.log(`Uploaded ${output}`);
          resolve(output);
        })
        .toFormat("mp4")
        .run();
    });
  });

  await Promise.all(promises);
}

main();
