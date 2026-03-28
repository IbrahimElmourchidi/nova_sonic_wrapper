// src/infrastructure/logging/WinstonLogger.ts
import "reflect-metadata";
import { injectable } from "tsyringe";
import winston from "winston";
import type { ILogger } from "./ILogger";

const { combine, timestamp, json, colorize, printf, errors } = winston.format;

/** Returns a UTC HH:mm:ss.SSS string matching Flutter/iOS log format. */
function utcHms(): string {
  const d = new Date();
  const h = String(d.getUTCHours()).padStart(2, "0");
  const m = String(d.getUTCMinutes()).padStart(2, "0");
  const s = String(d.getUTCSeconds()).padStart(2, "0");
  const ms = String(d.getUTCMilliseconds()).padStart(3, "0");
  return `${h}:${m}:${s}.${ms}`;
}

function createLogger(level: string, isProduction: boolean): winston.Logger {
  const devFormat = combine(
    errors({ stack: true }),
    timestamp({ format: () => utcHms() }),
    colorize(),
    printf(({ level, message, timestamp, ...meta }) => {
      const metaStr = Object.keys(meta).length
        ? " " + JSON.stringify(meta)
        : "";
      return `${timestamp} [${level}] ${message}${metaStr}`;
    })
  );

  const prodFormat = combine(errors({ stack: true }), timestamp({ format: () => new Date().toISOString() }), json());

  return winston.createLogger({
    level,
    format: isProduction ? prodFormat : devFormat,
    transports: [new winston.transports.Console()],
  });
}

@injectable()
export class WinstonLogger implements ILogger {
  private readonly logger: winston.Logger;

  constructor(level = "info", isProduction = false) {
    this.logger = createLogger(level, isProduction);
  }

  error(message: string, meta?: Record<string, unknown>): void {
    this.logger.error(message, meta);
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    this.logger.warn(message, meta);
  }

  info(message: string, meta?: Record<string, unknown>): void {
    this.logger.info(message, meta);
  }

  debug(message: string, meta?: Record<string, unknown>): void {
    this.logger.debug(message, meta);
  }
}
