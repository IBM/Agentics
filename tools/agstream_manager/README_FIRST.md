# 🚀 Quick Start Guide

## First Time Setup (Do This Once!)

### 1. Rebuild Docker Image with Dependencies

```bash
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh
```

This builds a custom Flink image with all Agentics dependencies (hnswlib, scikit-learn, etc.) **baked in**. You only need to do this once!

### 2. Start Services

```bash
./manage_services_full.sh start
```

## Generate Jokes from Questions

After setup, use the quick script:

```bash
./scripts/quick_joke_generation.sh
```

Then in Flink SQL:

```sql
-- Generate jokes
INSERT INTO J
SELECT JSON_VALUE(agmap('Joke', text), '$.joke_text') as joke_text
FROM Q;

-- View results
SELECT * FROM J LIMIT 10;
```

## Why Rebuild the Image?

**Problem**: Docker containers are ephemeral - they lose installed packages on restart.

**Solution**: Bake dependencies into the Docker image itself.

- ✅ Dependencies persist across restarts
- ✅ No manual installation needed
- ✅ Fast and reliable
- ✅ Only rebuild when dependencies change

## Daily Usage

```bash
# Start services
./manage_services_full.sh start

# Stop services
./manage_services_full.sh stop

# Restart services
./manage_services_full.sh restart
```

## Full Documentation

- **[SETUP_GUIDE.md](SETUP_GUIDE.md)** - Complete setup instructions
- **[GENERATE_JOKES_GUIDE.md](GENERATE_JOKES_GUIDE.md)** - AGmap UDF usage guide
- **[README.md](README.md)** - Full AGStream Manager documentation

## Troubleshooting

### "ModuleNotFoundError: No module named 'hnswlib'"

You need to rebuild the Docker image:

```bash
./scripts/rebuild_flink_image.sh
./manage_services_full.sh restart
```

### Services Won't Start

```bash
./manage_services_full.sh stop
./manage_services_full.sh start
```

## Architecture

```
Docker Image (Built Once)
  ↓
  Contains: Python + PyFlink + Agentics + hnswlib + all deps
  ↓
Docker Compose (Start/Stop Anytime)
  ↓
  Kafka + Flink + Schema Registry + UIs
  ↓
Auto-Install on Startup
  ↓
  Copy .env + Install UDFs
  ↓
Ready to Use! 🎉
