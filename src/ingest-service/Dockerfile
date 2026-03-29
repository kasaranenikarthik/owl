# Stage 1: Build Python dependencies
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends libgl1 libglib2.0-0 ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/deps /usr/local/lib/python3.11/site-packages/
COPY ingest.py .

# Generate a synthetic 60-second test video at build time.
# YOLO won't detect real objects in it, but it proves the pipeline works.
# Replace this with a real video: put your .mp4 in the build context and use
#   COPY your-video.mp4 /videos/test.mp4
RUN mkdir -p /videos && \
    ffmpeg -f lavfi -i testsrc=duration=60:size=640x480:rate=30 \
           -c:v libx264 -pix_fmt yuv420p /videos/test.mp4

CMD ["python", "ingest.py"]
