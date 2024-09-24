const fs = require("node:fs");
const path = require("node:path");
const ffmpeg = require("fluent-ffmpeg");
const fsPromises = require("node:fs/promises");
const { S3Client, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");

const { KEY, BUCKET, PROD_BUCKET, AWS_ACCESS_KEY_ID, AWS_DEFAULT_REGION, AWS_SECRET_ACCESS_KEY } = process.env;

const RESOLUTIONS = [
  { name: "144p", width: 256, height: 144, bitrate: "100k" },
  { name: "240p", width: 426, height: 240, bitrate: "300k" },
  { name: "360p", width: 640, height: 360, bitrate: "800k" },
  { name: "480p", width: 854, height: 480, bitrate: "1400k" },
  { name: "720p", width: 1280, height: 720, bitrate: "2800k" },
  { name: "1080p", width: 1920, height: 1080, bitrate: "4500k" },
  { name: "1440p", width: 2560, height: 1440, bitrate: "9000k" }, // 2K
  { name: "2160p", width: 3840, height: 2160, bitrate: "12000k" }, // 4K
  { name: "4320p", width: 7680, height: 4320, bitrate: "50000k" }, // 8K
];

const s3Client = new S3Client({
  region: AWS_DEFAULT_REGION,
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
});

function getFileExtension(fileName) {
  return path.extname(fileName);
}

function getFormattedKey(key) {
  return key.split(".")[0].replace("videos/", "").replace("//", "/");
}

async function uploadToS3(filePath, key) {
  try {
    const putCommand = new PutObjectCommand({
      Bucket: PROD_BUCKET,
      Key: key,
      Body: fs.createReadStream(filePath),
    });
    await s3Client.send(putCommand);
  } catch (error) {
    console.error(`Error uploading ${key}:`, error);
    throw error;
  }
}

async function getVideoMetadata(filePath) {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) reject(err);
      else
        resolve({
          duration: metadata.format.duration,
          width: metadata.streams[0].width,
          height: metadata.streams[0].height,
        });
    });
  });
}

async function uploadHLSFiles(outputDir, resolution) {
  try {
    const formattedKey = getFormattedKey(KEY);
    const files = await fsPromises.readdir(outputDir);
    const hlsFiles = files.filter(
      (file) => file.startsWith(resolution.name) && (file.endsWith(".m3u8") || file.endsWith(".ts"))
    );

    const uploadPromises = hlsFiles.map((file) =>
      uploadToS3(path.join(outputDir, file), `hls/${formattedKey}/${resolution.name}/${file}`)
    );

    await Promise.all(uploadPromises);
  } catch (error) {
    console.error(`Error uploading HLS files for resolution ${resolution.name}:`, error);
    throw error;
  }
}

function getValidResolutions(width, height) {
  return RESOLUTIONS.filter((resolution) => resolution.width <= width && resolution.height <= height);
}

async function main() {
  try {
    const fileExtension = getFileExtension(KEY);
    const formattedKey = getFormattedKey(KEY);

    const originalFilePath = `original-video${fileExtension}`;

    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: KEY,
    });
    const result = await s3Client.send(command);

    await fsPromises.writeFile(originalFilePath, result.Body);

    const originalVideoPath = path.resolve(originalFilePath);

    const { duration, width, height } = await getVideoMetadata(originalVideoPath);
    const validResolutions = getValidResolutions(width, height);
    const estimatedSegments = Math.ceil(duration / 10);
    const padding = estimatedSegments.toString().length;

    const outputDir = "hls_output";
    await fsPromises.mkdir(outputDir, { recursive: true });

    console.log(
      "Starting HLS transcoding and upload for the following resolutions: " +
        validResolutions.map((res) => res.name).join(", ")
    );

    for (let resolution of validResolutions) {
      try {
        await new Promise((resolve, reject) => {
          ffmpeg(originalVideoPath)
            .output(`${outputDir}/${resolution.name}.m3u8`)
            .addOptions([
              `-c:v libx264`,
              `-c:a aac`,
              `-b:v ${resolution.bitrate}`,
              `-b:a 128k`,
              `-vf scale=${resolution.width}:${resolution.height}`,
              `-f hls`,
              `-hls_time 10`,
              `-hls_list_size 0`,
              `-hls_segment_filename ${outputDir}/${resolution.name}_%0${padding}d.ts`,
            ])
            .on("end", async () => {
              try {
                await uploadHLSFiles(outputDir, resolution);
                console.log(`Successfully processed and uploaded for ${resolution.name}`);
                resolve();
              } catch (uploadError) {
                reject(uploadError);
              }
            })
            .on("error", (transcodeError) => {
              console.error(`Transcoding error for ${resolution.name}:`, transcodeError);
              reject(transcodeError);
            })
            .run();
        });
      } catch (error) {
        console.error(`Error during transcoding/uploading for ${resolution.name}. Stopping.`);
        process.exit(1);
      }
    }

    // Generate master playlist if all resolutions are processed successfully
    const masterPlaylist = "#EXTM3U\n#EXT-X-VERSION:3\n";
    const playlistEntries = RESOLUTIONS.filter(
      (resolution) => resolution.width <= width && resolution.height <= height
    ).map((resolution) => {
      const bandwidth = parseInt(resolution.bitrate) * 1000;
      const resolutionString = `${resolution.width}x${resolution.height}`;
      const playlistPath = `${resolution.name}/${resolution.name}.m3u8`;

      return `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${resolutionString}\n${playlistPath}`;
    });

    const masterPlaylistContent = masterPlaylist + playlistEntries.join("\n");

    const masterPlaylistPath = path.join(outputDir, "master.m3u8");
    await fsPromises.writeFile(masterPlaylistPath, masterPlaylistContent);

    await uploadToS3(masterPlaylistPath, `hls/${formattedKey}/master.m3u8`);

    console.log("HLS transcoding and upload complete for all resolutions.");
  } catch (error) {
    console.error("An unrecoverable error occurred:", error);
    process.exit(1);
  }
}

main().catch((error) => {
  console.error("Main process error:", error);
  process.exit(1);
});
