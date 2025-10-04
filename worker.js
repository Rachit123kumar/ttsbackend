require("dotenv").config();

// fetch fallback for Node < 18
let fetchFn = globalThis.fetch;
if (!fetchFn) {
  try {
    const nodeFetch = require("node-fetch");
    fetchFn = nodeFetch.default || nodeFetch;
  } catch (e) {
    console.warn("No global fetch and node-fetch not installed. Please install node-fetch or run Node >= 18.");
  }
}

const fs = require("fs/promises");
const path = require("path");
const os = require("os");
const { v4: uuidv4 } = require("uuid");
const ffmpeg = require("fluent-ffmpeg");
const ffmpegPath = require("ffmpeg-static");
const ffprobePath = require("@ffprobe-installer/ffprobe").path;
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const mongoose = require("mongoose");
const redis = require("redis");

ffmpeg.setFfmpegPath(ffmpegPath);
ffmpeg.setFfprobePath(ffprobePath);

// MongoDB connection - Fixed connection string
const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://hellobittukumar12_db_user:YurNjWDcOZqLix@ttoundbackend.5jspqxd.mongodb.net/video_jobs_db?retryWrites=true&w=majority&appName=ttsoundbackend";

async function connectDB() {
  try {
    await mongoose.connect(MONGO_URI);
    console.log("Worker: MongoDB connected successfully");
  } catch (error) {
    console.error("Worker: MongoDB connection error:", error);
    process.exit(1);
  }
}

// MongoDB Schema
const VideoJobSchema = new mongoose.Schema({
  jobId: { type: String, required: true, unique: true },
  status: { type: String, default: "pending" },
  videoUrl: { type: String },
  error: { type: String },
  images: [{
    url: String,
    start: Number,
    end: Number
  }],
  audioUrl: { type: String },
  transitionSec: { type: Number, default: 0 },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

const VideoJob = mongoose.model("VideoJob", VideoJobSchema);

// Redis client with better error handling
let redisClient;

async function createRedisClient() {
  const client = redis.createClient({
    url: process.env.REDIS_URL || "redis://localhost:6379"
  });

  client.on("error", (err) => console.log("Worker: Redis Client Error", err));
  client.on("connect", () => console.log("Worker: Redis connected"));
  client.on("ready", () => console.log("Worker: Redis ready"));
  
  await client.connect();
  return client;
}

// S3/R2 Config
const R2_ENDPOINT = process.env.CLOUDFLARE_R2_ENDPOINT;
const R2_BUCKET = process.env.CLOUDFLARE_R2_BUCKET;
const R2_ACCESS = process.env.CLOUDFLARE_R2_ACCESS_KEY;
const R2_SECRET = process.env.CLOUDFLARE_R2_SECRET_KEY;
const R2_PUBLIC_URL = process.env.CLOUDFLARE_R2_PUBLIC_URL;

const s3Client = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS,
    secretAccessKey: R2_SECRET,
  },
});

/* ---------- Helpers ---------- */
function tempPath(filename = "") {
  return path.join(os.tmpdir(), `${uuidv4()}-${filename}`);
}

async function downloadToFile(url, destPath) {
  if (!fetchFn) throw new Error("No fetch available (install node-fetch or use Node >= 18)");
  const res = await fetchFn(url);
  if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
  const arrayBuffer = await res.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  await fs.writeFile(destPath, buffer);
  return destPath;
}

/* ---------- Video Processing Function ---------- */
async function processVideoJob(jobData) {
  const { jobId, audioUrl, images, transitionSec = 0 } = jobData;
  
  try {
    // Update job status to processing
    await VideoJob.findOneAndUpdate(
      { jobId },
      { status: "processing", updatedAt: new Date() }
    );

    console.log(`Processing job ${jobId}`);

    // 1) Download audio
    const audioLocal = tempPath("audio.mp3");
    await downloadToFile(audioUrl, audioLocal);

    // 2) Download images and create clips (force 1080x1920)
    const tempVideos = [];
    const durations = [];

    for (let i = 0; i < images.length; i++) {
      const item = images[i];
      const duration = Math.max(0.2, Number(item.end) - Number(item.start) || 1.0);
      durations.push(duration);

      const imgLocal = tempPath(`img${i}`);
      await downloadToFile(item.url, imgLocal);

      const outVideo = tempPath(`clip${i}.mp4`);
      tempVideos.push(outVideo);

      // Create a consistent 1080x1920 clip
      await new Promise((resolve, reject) => {
        ffmpeg()
          .input(imgLocal)
          .inputOptions(['-loop 1'])
          .videoFilters([
            'scale=1080:-1:force_original_aspect_ratio=decrease',
            'pad=1080:1920:(ow-iw)/2:(oh-ih)/2',
            'setsar=1',
            'format=yuv420p'
          ])
          .outputOptions([
            `-t ${duration}`,
            '-r 30',
            '-c:v libx264',
            '-preset veryfast',
            '-crf 23',
            '-pix_fmt yuv420p',
            '-movflags +faststart'
          ])
          .on('start', cmd => console.log(`ffmpeg start (clip ${i}): ${cmd}`))
          .on('stderr', line => {})
          .on('error', (err, stdout, stderr) => {
            console.error(`ffmpeg error while creating clip ${i}:`, err.message);
            reject(err);
          })
          .on('end', () => {
            console.log(`Created clip ${i} -> ${outVideo}`);
            resolve();
          })
          .save(outVideo);
      });

      // remove local image copy
      await fs.unlink(imgLocal).catch(() => {});
    }

    // 3) Concatenate clips into single video and add audio
    const concatListPath = tempPath("concat.txt");
    const concatContent = tempVideos.map(tv => `file '${tv.replace(/\\/g, "/")}'`).join("\n");
    await fs.writeFile(concatListPath, concatContent, "utf8");

    const finalLocal = tempPath("final.mp4");

    await new Promise((resolve, reject) => {
      ffmpeg()
        .input(concatListPath)
        .inputOptions(['-f concat', '-safe 0'])
        .input(audioLocal)
        .outputOptions([
          '-map 0:v',
          '-map 1:a',
          '-c:v libx264',
          '-r 30',
          '-pix_fmt yuv420p',
          '-c:a aac',
          '-b:a 128k',
          '-shortest',
          '-movflags +faststart'
        ])
        .on('start', cmd => console.log('ffmpeg start (concat):', cmd))
        .on('stderr', line => {})
        .on('error', (err, stdout, stderr) => {
          console.error('ffmpeg concat error:', err.message);
          reject(err);
        })
        .on('end', () => {
          console.log('Created final video ->', finalLocal);
          resolve();
        })
        .save(finalLocal);
    });

    // 4) Upload final video to R2
    const finalBuffer = await fs.readFile(finalLocal);
    const key = `videos/${uuidv4()}.mp4`;
    await s3Client.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: key,
      Body: finalBuffer,
      ContentType: "video/mp4",
    }));

    const publicFinalUrl = `${R2_PUBLIC_URL}/${key}`;

    // 5) Update job with success
    await VideoJob.findOneAndUpdate(
      { jobId },
      { 
        status: "completed", 
        videoUrl: publicFinalUrl,
        updatedAt: new Date()
      }
    );

    console.log(`Job ${jobId} completed successfully`);

    // 6) cleanup
    const cleanupPaths = [audioLocal, concatListPath, finalLocal, ...tempVideos];
    await Promise.all(cleanupPaths.map(p => fs.unlink(p).catch(() => {})));

  } catch (err) {
    console.error(`Error processing job ${jobId}:`, err);
    
    // Update job with error
    await VideoJob.findOneAndUpdate(
      { jobId },
      { 
        status: "failed", 
        error: err.message || String(err),
        updatedAt: new Date()
      }
    );
  }
}

/* ---------- Worker Main Loop ---------- */
async function startWorker() {
  try {
    await connectDB();
    redisClient = await createRedisClient();
    
    console.log("Worker started, waiting for jobs...");

    while (true) {
      try {
        // BRPOP will block until a job is available
        const result = await redisClient.brPop("video-jobs", 0);
        
        if (result && result.element) {
          const jobData = JSON.parse(result.element);
          await processVideoJob(jobData);
        }
      } catch (err) {
        console.error("Error in worker loop:", err);
        // Wait a bit before retrying to avoid tight loop on errors
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
  } catch (err) {
    console.error("Worker failed to start:", err);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on("SIGINT", async () => {
  console.log("Shutting down worker...");
  if (redisClient) await redisClient.quit();
  await mongoose.connection.close();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("Shutting down worker...");
  if (redisClient) await redisClient.quit();
  await mongoose.connection.close();
  process.exit(0);
});

// Start the worker
startWorker();