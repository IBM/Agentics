# AGStream Manager Setup Guide

## One-Time Setup (Required)

### Step 1: Rebuild Docker Image with All Dependencies

The Flink Docker image needs to be rebuilt once to include all Agentics dependencies (hnswlib, scikit-learn, etc.):

```bash
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh
```

This builds a custom Flink image with:
- ✅ All Agentics dependencies pre-installed
- ✅ UDFs ready to use
- ✅ No manual installation needed after restarts

**You only need to do this once** (or when dependencies change).

### Step 2: Start Services

```bash
./manage_services_full.sh start
```

This will:
- Start Kafka, Schema Registry, Flink, and all UIs
- Auto-copy .env file with API keys to Flink containers
- Auto-install UDFs (lightweight, happens on every start)

## Daily Usage

### Starting Services

```bash
cd tools/agstream_manager
./manage_services_full.sh start
```

### Stopping Services

```bash
./manage_services_full.sh stop
```

### Restarting Services

```bash
./manage_services_full.sh restart
```

### Clean Restart (Clear All Data)

```bash
./manage_services_full.sh clean-restart
```

## Using AGmap UDF for Joke Generation

### Quick Start

```bash
cd tools/agstream_manager
./scripts/quick_joke_generation.sh
```

This opens Flink SQL with everything configured. Then run:

```sql
-- Test with single question
SELECT agmap('Joke', 'Why did the chicken cross the road?') FROM Q LIMIT 1;

-- Generate jokes from all questions
INSERT INTO J
SELECT JSON_VALUE(agmap('Joke', text), '$.joke_text') as joke_text
FROM Q;

-- View results
SELECT * FROM J LIMIT 10;
```

## Why This Approach?

### Before (Manual Installation Every Time)
- ❌ Had to run `install_agentics_in_flink.sh` after every restart
- ❌ Dependencies lost when containers restarted
- ❌ Slow and error-prone

### After (Docker Image with Dependencies)
- ✅ Dependencies baked into Docker image
- ✅ Persist across restarts
- ✅ Fast and reliable
- ✅ Only rebuild image when dependencies change

## Troubleshooting

### "ModuleNotFoundError: No module named 'hnswlib'"

This means the Docker image wasn't rebuilt with dependencies. Run:

```bash
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh
./manage_services_full.sh restart
```

### Services Won't Start

Check if ports are in use:
```bash
./manage_services_full.sh stop
./manage_services_full.sh start
```

### UDF Not Found

UDFs are auto-installed on startup. If missing, manually install:
```bash
./scripts/install_udfs.sh
```

### API Keys Not Working

Make sure your `.env` file exists at the project root with:
```
OPENAI_API_KEY=your_key_here
# OR
ANTHROPIC_API_KEY=your_key_here
```

The manage_services script auto-copies this to Flink containers.

## Architecture

```
┌─────────────────────────────────────────┐
│  Custom Flink Docker Image              │
│  - Python 3.10                          │
│  - PyFlink 1.18.1                       │
│  - Agentics source code                 │
│  - All dependencies (hnswlib, etc.)     │
│  - UDFs pre-copied                      │
└─────────────────────────────────────────┘
           ↓
┌─────────────────────────────────────────┐
│  Docker Compose Services                │
│  - Kafka                                │
│  - Schema Registry (Karapace)           │
│  - Flink JobManager                     │
│  - Flink TaskManager                    │
│  - Kafka UI                             │
│  - Schema Registry UI                   │
└─────────────────────────────────────────┘
           ↓
┌─────────────────────────────────────────┐
│  Auto-Installation on Startup           │
│  - Copy .env to containers              │
│  - Install UDFs (if needed)             │
│  - Ready to use!                        │
└─────────────────────────────────────────┘
```

## Access Points

After starting services:

- **Kafka**: localhost:9092
- **Schema Registry**: http://localhost:8081
- **Kafka UI**: http://localhost:8080
- **Schema Registry UI**: http://localhost:8000
- **Flink Web UI**: http://localhost:8085
- **AGStream Manager**: http://localhost:5003

## Next Steps

1. ✅ Rebuild Docker image (one-time)
2. ✅ Start services
3. ✅ Use AGmap UDF for transformations
4. 📖 Read [GENERATE_JOKES_GUIDE.md](GENERATE_JOKES_GUIDE.md) for detailed examples
