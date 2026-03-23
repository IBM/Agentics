# Build Optimization Guide

## Overview

This guide explains how to efficiently update UDFs and rebuild containers without unnecessary full rebuilds.

## Quick Reference

| Scenario | Command | Speed | Use When |
|----------|---------|-------|----------|
| **UDF changes only** | `./scripts/update_udf.sh` | ⚡ Instant | Modified UDF files |
| **Incremental build** | `./scripts/rebuild_flink_image.sh` | 🚀 Fast | Added dependencies |
| **Full rebuild** | `./scripts/rebuild_flink_image.sh --force` | 🐌 Slow | Major changes |

## Detailed Workflows

### 1. UDF-Only Updates (Fastest) ⚡

When you only modify UDF files (e.g., `udfs/ag_operators.py`, `udfs/agreduce.py`):

```bash
# Just copy the updated UDF to the running container
cd tools/agstream_manager
./scripts/update_udf.sh
```

**What it does:**
- Copies updated UDF files to the running Flink container
- No container restart needed
- Changes take effect immediately on next query

**Time:** ~1 second

**Use when:**
- Modified UDF logic
- Added new functions to UDF files
- Fixed bugs in existing UDFs

### 2. Incremental Build (Default) 🚀

When you need to rebuild but want to use Docker cache:

```bash
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh
```

**What it does:**
- Rebuilds Docker image using layer cache
- Only rebuilds changed layers
- Much faster than full rebuild

**Time:** ~30 seconds - 2 minutes (depending on changes)

**Use when:**
- Added new Python dependencies
- Modified Dockerfile
- Updated Agentics package
- Changed environment configuration

### 3. Full Rebuild (Slowest) 🐌

When you need a complete rebuild from scratch:

```bash
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh --force
```

**What it does:**
- Rebuilds entire Docker image without cache
- Downloads all dependencies fresh
- Ensures clean state

**Time:** ~5-10 minutes

**Use when:**
- Troubleshooting cache issues
- Major version upgrades
- Suspected corrupted layers
- First-time setup

## Workflow Examples

### Example 1: Adding a New UDF Function

```bash
# 1. Edit the UDF file
vim tools/agstream_manager/udfs/agmap_row.py

# 2. Quick update (no rebuild)
cd tools/agstream_manager
./scripts/update_udf.sh

# 3. Test immediately
./scripts/sql_shell.sh
```

### Example 2: Adding a Python Dependency

```bash
# 1. Edit requirements or Dockerfile
vim tools/agstream_manager/Dockerfile.flink-python

# 2. Incremental rebuild (uses cache)
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh

# 3. Restart services
./manage_services_full.sh restart
```

### Example 3: Major Update or Troubleshooting

```bash
# 1. Make your changes
vim tools/agstream_manager/Dockerfile.flink-python

# 2. Full rebuild (no cache)
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh --force

# 3. Restart services
./manage_services_full.sh restart
```

## Understanding Docker Cache

### How Cache Works

Docker caches each layer of the image:
- ✅ Unchanged layers are reused
- 🔄 Changed layers and all subsequent layers are rebuilt
- 💾 Cache is stored locally

### Cache Invalidation

A layer is rebuilt when:
- The Dockerfile instruction changes
- Any file copied in that layer changes
- A previous layer was rebuilt

### Optimizing Cache Usage

**Good Dockerfile structure:**
```dockerfile
# 1. Base image (rarely changes)
FROM flink:1.18.1

# 2. System dependencies (rarely changes)
RUN apt-get update && apt-get install -y ...

# 3. Python dependencies (occasionally changes)
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# 4. Application code (frequently changes)
COPY udfs/ /opt/flink/udfs/
```

This structure ensures that frequent code changes don't invalidate the expensive dependency installation layers.

## Performance Comparison

| Method | Time | Network | Disk I/O | CPU |
|--------|------|---------|----------|-----|
| UDF Update | 1s | None | Minimal | Minimal |
| Incremental | 30s-2m | Minimal | Moderate | Moderate |
| Full Rebuild | 5-10m | Heavy | Heavy | Heavy |

## Best Practices

### 1. Development Workflow

During active development:
```bash
# Make UDF changes
vim udfs/agmap_row.py

# Quick update
./scripts/update_udf.sh

# Test
./scripts/sql_shell.sh
```

### 2. Dependency Updates

When adding dependencies:
```bash
# Update requirements
vim requirements.txt

# Incremental rebuild
./scripts/rebuild_flink_image.sh

# Restart
./manage_services_full.sh restart
```

### 3. Clean State

When troubleshooting:
```bash
# Full rebuild
./scripts/rebuild_flink_image.sh --force

# Clean restart
./manage_services_full.sh down
./manage_services_full.sh up
```

## Troubleshooting

### "Changes not taking effect"

Try in order:
1. `./scripts/update_udf.sh` - Quick UDF update
2. `./scripts/rebuild_flink_image.sh` - Incremental rebuild
3. `./scripts/rebuild_flink_image.sh --force` - Full rebuild

### "Build is slow"

- Use incremental build (default)
- Check Docker cache: `docker system df`
- Clean old images: `docker image prune`

### "Out of disk space"

```bash
# Clean up Docker
docker system prune -a

# Then rebuild
./scripts/rebuild_flink_image.sh --force
```

## Scripts Reference

### update_udf.sh
```bash
./scripts/update_udf.sh
```
- Copies UDF to running container
- No rebuild needed
- Instant updates

### rebuild_flink_image.sh
```bash
# Incremental (default)
./scripts/rebuild_flink_image.sh

# Full rebuild
./scripts/rebuild_flink_image.sh --force
```
- Rebuilds Docker image
- `--force` disables cache
- Requires service restart

### install_agmap_row.sh
```bash
./scripts/install_agmap_row.sh
```
- Registers UDF in Flink SQL
- Run after container restart
- Idempotent (safe to run multiple times)

## Summary

**For UDF changes:** Use `update_udf.sh` (instant)
**For dependency changes:** Use `rebuild_flink_image.sh` (fast)
**For troubleshooting:** Use `rebuild_flink_image.sh --force` (slow but thorough)

---

**Pro Tip:** During development, keep the containers running and use `update_udf.sh` for rapid iteration!
