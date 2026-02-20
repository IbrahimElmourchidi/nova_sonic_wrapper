# Nova Sonic Server — Clean Architecture

Production-grade Node.js server wrapping the **Amazon Nova Sonic** bidirectional speech-to-speech API via AWS Bedrock. Built with SOLID principles, separated layers, Zod validation, tsyringe DI, and designed for local development + GCP Cloud Run deployment.

---

## Architecture

```
src/
├── domain/                      # Enterprise business rules
│   ├── entities/Session.ts      # Session aggregate
│   ├── errors/index.ts          # Typed domain errors
│   ├── repositories/            # Abstract interfaces
│   │   └── ISessionRepository.ts
│   ├── services/                # Abstract service interfaces
│   │   ├── IStreamingService.ts
│   │   └── IToolService.ts
│   └── types.ts                 # Shared value types
│
├── application/                 # Application use cases (orchestration)
│   ├── dtos/SessionDtos.ts      # Zod schemas + inferred types
│   └── use-cases/
│       ├── SessionUseCase.ts    # Session lifecycle orchestration
│       └── AudioStreamUseCase.ts # Audio buffer management + back-pressure
│
├── infrastructure/              # Frameworks, drivers, external services
│   ├── bedrock/
│   │   └── BedrockStreamingService.ts  # AWS Bedrock implementation
│   ├── config/
│   │   ├── AppConfig.ts         # Zod-validated env config
│   │   ├── container.ts         # tsyringe DI container wiring
│   │   ├── defaults.ts          # Audio/inference defaults
│   │   └── tokens.ts            # DI token symbols
│   ├── logging/
│   │   ├── ILogger.ts           # Logger interface
│   │   └── WinstonLogger.ts     # Winston implementation
│   ├── repositories/
│   │   └── InMemorySessionRepository.ts
│   └── tools/
│       └── ToolService.ts       # Tool execution (weather, datetime)
│
└── presentation/                # Delivery mechanisms
    ├── http/
    │   ├── App.ts               # Express + Socket.IO composition root
    │   └── HealthRouter.ts      # /health endpoint
    └── websocket/
        └── SocketGateway.ts     # Socket.IO event handlers
```

### Layer dependency rule
```
presentation → application → domain ← infrastructure
```
The domain layer has **zero** external dependencies.

---

## SOLID Principles Applied

| Principle | Where |
|-----------|-------|
| **S**ingle Responsibility | Each class has one reason to change: `AudioStreamUseCase` only manages audio queues, `SessionUseCase` only orchestrates session lifecycle |
| **O**pen/Closed | New tools extend `ToolService` by adding to a handler map, no existing code changes |
| **L**iskov Substitution | `BedrockStreamingService` and `InMemorySessionRepository` are drop-in replacements for their interfaces |
| **I**nterface Segregation | `IStreamingService` and `IToolService` are focused, minimal interfaces |
| **D**ependency Inversion | All concrete classes depend on abstractions injected via tsyringe |

---

## Setup

### Prerequisites
- Node.js 20+
- AWS credentials with Bedrock access

### Install
```bash
cp .env.example .env
# Edit .env with your AWS config

npm install
```

### Run locally
```bash
npm run dev
```

### Build
```bash
npm run build
npm start
```

### Environment variables
See `.env.example` for all options. Required at minimum:
- `AWS_REGION` (default: `us-east-1`)
- AWS credentials via profile (`AWS_PROFILE`) or environment (`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`)

---

## WebSocket Events

### Client → Server
| Event | Payload | Description |
|-------|---------|-------------|
| `initializeConnection` | — | Create session + start Bedrock stream |
| `promptStart` | — | Enqueue session + prompt start events |
| `systemPrompt` | `{ content: string }` | Set system prompt |
| `audioStart` | `{ audioConfig? }` | Configure + open audio content |
| `audioInput` | `Buffer \| base64 string` | Stream audio chunk |
| `stopAudio` | — | Graceful teardown |
| `startNewChat` | — | Close current session, open new one |

### Server → Client
| Event | Description |
|-------|-------------|
| `audioReady` | Audio is configured and streaming can begin |
| `audioOutput` | Audio chunk from model |
| `textOutput` | Transcript text |
| `contentStart` | New content block started |
| `contentEnd` | Content block ended |
| `toolUse` | Tool being invoked |
| `toolResult` | Tool execution result |
| `streamComplete` | Session complete |
| `sessionClosed` | Teardown acknowledged |
| `error` | `{ message, code?, details? }` |

---

## GCP Deployment

```bash
export GCP_PROJECT_ID=my-project
export GCP_REGION=us-central1
export AWS_REGION=us-east-1

chmod +x deploy-gcp.sh
./deploy-gcp.sh
```

The app runs on Cloud Run as a stateless container. AWS credentials should be provided via [Secret Manager](https://cloud.google.com/secret-manager) or Workload Identity for production workloads.

> **Note:** Cloud Run uses HTTP/2 by default which is required by AWS SDK's bidirectional streaming. No special configuration needed.

---

## Adding a New Tool

1. Add a handler method in `ToolService`:
```typescript
private async handleMyTool(input: unknown): Promise<Record<string, unknown>> {
  // validate input with Zod, execute, return result
}
```

2. Register it in the constructor map:
```typescript
this.handlers = new Map([
  ...existingHandlers,
  ["mytool", this.handleMyTool.bind(this)],
]);
```

3. Add the `toolSpec` in `BedrockStreamingService.enqueuePromptStart()`.

No other files change.


gcloud run deploy nova-sonic-server \
    --source . \
    --region europe-west3 \
    --set-secrets="AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID:latest,AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY:latest" \
    --set-env-vars="AWS_REGION=us-east-1,NODE_ENV=production" \
    --allow-unauthenticated \
    --memory 512Mi \
    --cpu 1 \
    --concurrency 80 \
    --timeout 300


    gcloud run deploy nova-sonic-server     --source .     --region europe-west3     --set-secrets="AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID:latest,AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY:latest"     --set-env-vars="AWS_REGION=us-east-1,NODE_ENV=production"     --allow-unauthenticated     --memory 512Mi     --cpu 1     --concurrency 80     --timeout 300