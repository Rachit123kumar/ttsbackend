require("dotenv").config();
const express = require("express");
const { v4: uuidv4 } = require("uuid");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const cors = require("cors");
const multer = require("multer");
const mongoose = require("mongoose");
const redis = require("redis");

// MongoDB connection - Fixed connection string
const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://hellobittukumar12_db_user:YurNjWDcOZqLix@ttoundbackend.5jspqxd.mongodb.net/video_jobs_db?retryWrites=true&w=majority&appName=ttsoundbackend";

async function connectDB() {
  try {
    await mongoose.connect(MONGO_URI);
    console.log("MongoDB connected successfully");
  } catch (error) {
    console.error("MongoDB connection error:", error);
    process.exit(1);
  }
}

// MongoDB Schema
const VideoJobSchema = new mongoose.Schema({
  jobId: { type: String, required: true, unique: true },
  status: { type: String, default: "pending" }, // pending, processing, completed, failed
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

  client.on("error", (err) => console.log("Redis Client Error", err));
  client.on("connect", () => console.log("Redis connected"));
  client.on("ready", () => console.log("Redis ready"));
  
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

// Express setup
const app = express();
app.use(express.json({ limit: "250mb" }));
app.use(cors());

// Multer setup
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 30 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (/^image\/(png|jpeg|jpg|webp|gif)$/i.test(file.mimetype)) cb(null, true);
    else cb(new Error("Only image files are allowed (png, jpg, webp, gif)."));
  }
});

// Route: Upload image to R2
app.post("/upload-image", upload.single("image"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "No file uploaded" });

    const file = req.file;
    const mime = file.mimetype;
    const ext = (mime.split("/")[1] || "png").replace("jpeg", "jpg");
    const key = `images/${uuidv4()}.${ext}`;

    await s3Client.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: key,
      Body: file.buffer,
      ContentType: mime,
    }));

    const publicUrl = `${R2_PUBLIC_URL}/${key}`;
    return res.json({ url: publicUrl });
  } catch (err) {
    console.error("upload-image error:", err);
    return res.status(500).json({ error: err.message || String(err) });
  }
});

// Route: Submit video creation job
app.post("/create-video", async (req, res) => {
  try {
    const { audioUrl, images, transitionSec = 0 } = req.body;
    
    if (!audioUrl || !images || !Array.isArray(images) || images.length === 0) {
      return res.status(400).json({ error: "Missing audioUrl or images" });
    }

    const jobId = uuidv4();

    // Create job in MongoDB
    const videoJob = new VideoJob({
      jobId,
      status: "pending",
      images,
      audioUrl,
      transitionSec
    });

    await videoJob.save();

    // Add job to Redis queue
    const jobData = {
      jobId,
      audioUrl,
      images,
      transitionSec
    };

    await redisClient.lPush("video-jobs", JSON.stringify(jobData));

    return res.json({ 
      jobId,
      status: "pending",
      message: "Video creation job submitted successfully"
    });
  } catch (err) {
    console.error("create-video error:", err);
    return res.status(500).json({ error: err.message || String(err) });
  }
});

// Route: Check video job status
app.get("/job-status/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;

    const job = await VideoJob.findOne({ jobId });
    
    if (!job) {
      return res.status(404).json({ error: "Job not found" });
    }

    return res.json({
      jobId: job.jobId,
      status: job.status,
      videoUrl: job.videoUrl,
      error: job.error,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt
    });
  } catch (err) {
    console.error("job-status error:", err);
    return res.status(500).json({ error: err.message || String(err) });
  }
});

// Health check endpoint
app.get("/health", async (req, res) => {
  try {
    // Check MongoDB connection
    await mongoose.connection.db.admin().ping();
    
    // Check Redis connection
    await redisClient.ping();
    
    res.json({ 
      status: "healthy", 
      mongodb: "connected", 
      redis: "connected" 
    });
  } catch (err) {
    res.status(500).json({ 
      status: "unhealthy", 
      error: err.message 
    });
  }
});

// Start server
const PORT = process.env.PORT || 4000;

async function startServer() {
  try {
    await connectDB();
    redisClient = await createRedisClient();
    
    app.listen(PORT, () => {
      console.log(`Backend server listening on port ${PORT}`);
      console.log(`Health check available at http://localhost:${PORT}/health`);
    });
  } catch (err) {
    console.error("Failed to start server:", err);
    process.exit(1);
  }
}

startServer();