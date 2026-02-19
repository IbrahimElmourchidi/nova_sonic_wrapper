// src/infrastructure/tools/ToolService.ts
import "reflect-metadata";
import { injectable } from "tsyringe";
import { z } from "zod";

import type { IToolService } from "../../domain/services/IToolService";
import { ToolNotSupportedError } from "../../domain/errors";

// ── Validation schemas for tool inputs ──────────────────────────────────────

const WeatherInputSchema = z.object({
  content: z
    .string()
    .transform((str) => JSON.parse(str))
    .pipe(
      z.object({
        latitude: z.coerce.number(),
        longitude: z.coerce.number(),
      })
    ),
});

// ── Open-Meteo weather response schema ───────────────────────────────────────

const WeatherResponseSchema = z.object({
  current_weather: z.object({
    temperature: z.number(),
    windspeed: z.number(),
    weathercode: z.number(),
    is_day: z.number(),
    time: z.string(),
  }),
});

type ToolHandler = (input: unknown) => Promise<Record<string, unknown>>;

@injectable()
export class ToolService implements IToolService {
  private readonly handlers: Map<string, ToolHandler>;

  constructor() {
    this.handlers = new Map([
      ["getdateandtimetool", this.handleDateTime.bind(this)],
      ["getweathertool", this.handleWeather.bind(this)],
    ]);
  }

  supports(toolName: string): boolean {
    return this.handlers.has(toolName.toLowerCase());
  }

  async execute(
    toolName: string,
    toolInput: unknown
  ): Promise<Record<string, unknown>> {
    const key = toolName.toLowerCase();
    const handler = this.handlers.get(key);

    if (!handler) {
      throw new ToolNotSupportedError(toolName);
    }

    return handler(toolInput);
  }

  // ── Tool implementations ─────────────────────────────────────────────────

  private async handleDateTime(_input: unknown): Promise<Record<string, unknown>> {
    const date = new Date().toLocaleString("en-US", {
      timeZone: "America/Los_Angeles",
    });
    const pstDate = new Date(date);

    return {
      date: pstDate.toISOString().split("T")[0],
      year: pstDate.getFullYear(),
      month: pstDate.getMonth() + 1,
      day: pstDate.getDate(),
      dayOfWeek: pstDate
        .toLocaleString("en-US", { weekday: "long" })
        .toUpperCase(),
      timezone: "PST",
      formattedTime: pstDate.toLocaleTimeString("en-US", {
        hour12: true,
        hour: "2-digit",
        minute: "2-digit",
      }),
    };
  }

  private async handleWeather(
    input: unknown
  ): Promise<Record<string, unknown>> {
    const parsed = WeatherInputSchema.safeParse(input);
    if (!parsed.success) {
      throw new Error(
        `Invalid weather tool input: ${parsed.error.message}`
      );
    }

    const { latitude, longitude } = parsed.data.content;
    return this.fetchWeather(latitude, longitude);
  }

  private async fetchWeather(
    latitude: number,
    longitude: number
  ): Promise<Record<string, unknown>> {
    const url = new URL("https://api.open-meteo.com/v1/forecast");
    url.searchParams.set("latitude", String(latitude));
    url.searchParams.set("longitude", String(longitude));
    url.searchParams.set("current_weather", "true");

    const response = await fetch(url.toString(), {
      headers: {
        "User-Agent": "NovaSonicServer/1.0",
        Accept: "application/json",
      },
      signal: AbortSignal.timeout(10_000),
    });

    if (!response.ok) {
      throw new Error(
        `Weather API returned ${response.status}: ${response.statusText}`
      );
    }

    const raw = await response.json();
    const validated = WeatherResponseSchema.safeParse(raw);

    if (!validated.success) {
      // Return raw on validation failure — API schema may change
      return { weather_data: raw };
    }

    return { weather_data: validated.data };
  }
}