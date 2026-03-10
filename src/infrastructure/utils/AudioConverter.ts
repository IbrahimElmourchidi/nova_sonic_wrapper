// src/infrastructure/utils/AudioConverter.ts
import ffmpeg from "fluent-ffmpeg";
import ffmpegStatic from "ffmpeg-static";
import { createReadStream } from "node:fs";
import { PassThrough } from "node:stream";

// Point fluent-ffmpeg at the bundled binary — no system ffmpeg needed
ffmpeg.setFfmpegPath(ffmpegStatic as string);

/**
 * Converts any audio file (MP3, WAV, OGG, etc.) to raw LPCM
 * matching Nova Sonic's audioInput requirements:
 *   - Codec:       pcm_s16le  (16-bit signed little-endian)
 *   - Sample rate: 16,000 Hz
 *   - Channels:    1 (mono)
 *   - Container:   raw s16le (no WAV header)
 */
export function convertAudioFileToLpcm(
  inputPath: string,
  sampleRate = 16000
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    const pass = new PassThrough();

    ffmpeg(createReadStream(inputPath))
      .audioCodec("pcm_s16le")
      .audioFrequency(sampleRate)
      .audioChannels(1)
      .format("s16le")
      .on("error", (err: Error) =>
        reject(new Error(`FFmpeg conversion failed: ${err.message}`))
      )
      .pipe(pass);

    pass.on("data", (chunk: Buffer) => chunks.push(chunk));
    pass.on("end", () => resolve(Buffer.concat(chunks)));
    pass.on("error", reject);
  });
}