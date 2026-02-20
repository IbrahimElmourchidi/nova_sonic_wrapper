# ── Build stage ──────────────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./
COPY tsconfig.json ./

# Install ALL deps (including devDeps) so TypeScript compiler is available
RUN npm ci

COPY src/ ./src/

RUN npm run build

# ── Production stage ──────────────────────────────────────────────────────────
FROM node:20-alpine AS runner

WORKDIR /app

COPY package*.json ./

# Production deps only in the final image
RUN npm ci --only=production

# Copy compiled output from builder
COPY --from=builder /app/dist ./dist

# Cloud Run injects $PORT at runtime (default 8080); app reads it via AppConfig
EXPOSE 8080

CMD ["node", "dist/main.js"]
