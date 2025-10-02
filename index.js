// stitch-server/server.js
require("dotenv").config();

const express = require("express");

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
const cors = require("cors");
const multer = require("multer");

ffmpeg.setFfmpegPath(ffmpegPath);
ffmpeg.setFfprobePath(ffprobePath);

/* ---------- Config ---------- */
const R2_ENDPOINT = process.env.CLOUDFLARE_R2_ENDPOINT;
const R2_BUCKET = process.env.CLOUDFLARE_R2_BUCKET;
const R2_ACCESS = process.env.CLOUDFLARE_R2_ACCESS_KEY;
const R2_SECRET = process.env.CLOUDFLARE_R2_SECRET_KEY;
const R2_PUBLIC_URL = process.env.CLOUDFLARE_R2_PUBLIC_URL;

if (!R2_ENDPOINT || !R2_BUCKET || !R2_ACCESS || !R2_SECRET || !R2_PUBLIC_URL) {
  console.warn("Warning: Some Cloudflare R2 env vars may be missing. Uploads will fail until configured.");
}

/* ---------- S3 client ---------- */
const s3Client = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS,
    secretAccessKey: R2_SECRET,
  },
});

/* ---------- Express setup ---------- */
const app = express();
app.use(express.json({ limit: "250mb" }));
app.use(cors());

/* ---------- Multer (memory) ---------- */
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 30 * 1024 * 1024 }, // 30 MB
  fileFilter: (req, file, cb) => {
    if (/^image\/(png|jpeg|jpg|webp|gif)$/i.test(file.mimetype)) cb(null, true);
    else cb(new Error("Only image files are allowed (png, jpg, webp, gif)."));
  }
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

/* ---------- Route: upload-image ---------- */
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

/* ---------- Route: stitch ---------- */
/**
 * POST /stitch
 * body: { audioUrl: string, images: [{ url, start, end }, ...], transitionSec?: number (optional) }
 */
app.post("/stitch", async (req, res) => {
  try {
    const { audioUrl, images, transitionSec = 0 } = req.body;
    if (!audioUrl || !images || !Array.isArray(images) || images.length === 0) {
      return res.status(400).json({ error: "Missing audioUrl or images" });
    }

    // 1) Download audio
    const audioLocal = tempPath("audio.mp3");
    await downloadToFile(audioUrl, audioLocal);

    // 2) Download images and create clips (force 1080x1920)
    const tempVideos = [];
    const durations = [];

    for (let i = 0; i < images.length; i++) {
      const item = images[i];
      // calculate duration. fallback to 1.0 if not parseable
      const duration = Math.max(0.2, Number(item.end) - Number(item.start) || 1.0);
      durations.push(duration);

      const imgLocal = tempPath(`img${i}`);
      await downloadToFile(item.url, imgLocal);

      const outVideo = tempPath(`clip${i}.mp4`);
      tempVideos.push(outVideo);

      // Create a consistent 1080x1920 clip:
      // - scale to fit width 1080 (keep aspect), then pad vertically to 1920 (centered)
      // - setsar=1 and format ensures correct pixel format
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
            `-t ${duration}`,   // duration per clip
            '-r 30',            // fps
            '-c:v libx264',
            '-preset veryfast',
            '-crf 23',
            '-pix_fmt yuv420p',
            '-movflags +faststart'
          ])
          .on('start', cmd => console.log(`ffmpeg start (clip ${i}): ${cmd}`))
          .on('stderr', line => {
            // Uncomment to debug ffmpeg logs:
            // console.log(`ffmpeg stderr (clip ${i}): ${line}`);
          })
          .on('error', (err, stdout, stderr) => {
            console.error(`ffmpeg error while creating clip ${i}:`, err.message);
            if (stdout) console.error("ffmpeg stdout:", stdout);
            if (stderr) console.error("ffmpeg stderr:", stderr);
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
    // concat demuxer requires forward slashes in the paths on Windows too; ensure safe 0 for absolute paths
    const concatContent = tempVideos.map(tv => `file '${tv.replace(/\\/g, "/")}'`).join("\n");
    await fs.writeFile(concatListPath, concatContent, "utf8");

    const finalLocal = tempPath("final.mp4");

    await new Promise((resolve, reject) => {
      ffmpeg()
        .input(concatListPath)
        .inputOptions(['-f concat', '-safe 0'])
        .input(audioLocal)
        .outputOptions([
          '-map 0:v',     // take video from concat file
          '-map 1:a',     // take audio from audio file
          '-c:v libx264',
          '-r 30',
          '-pix_fmt yuv420p',
          '-c:a aac',
          '-b:a 128k',
          '-shortest',    // ends when shortest stream ends (usually audio or video)
          '-movflags +faststart'
        ])
        .on('start', cmd => console.log('ffmpeg start (concat):', cmd))
        .on('stderr', line => {
          // console.log('ffmpeg stderr (concat):', line);
        })
        .on('error', (err, stdout, stderr) => {
          console.error('ffmpeg concat error:', err.message);
          if (stdout) console.error('ffmpeg stdout:', stdout);
          if (stderr) console.error('ffmpeg stderr:', stderr);
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

    // 5) cleanup
    const cleanupPaths = [audioLocal, concatListPath, finalLocal, ...tempVideos];
    await Promise.all(cleanupPaths.map(p => fs.unlink(p).catch(() => {})));

    return res.json({ videoUrl: publicFinalUrl });
  } catch (err) {
    console.error("stitch error:", err);
    return res.status(500).json({ error: err.message || String(err) });
  }
});

/* ---------- Start ---------- */
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`Stitch server listening on ${PORT}`));
