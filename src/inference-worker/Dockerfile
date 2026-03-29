# Uses NVIDIA CUDA base image so PyTorch can access the GPU.
# The ECS GPU-optimized AMI + resourceRequirements GPU=1 in the
# task definition handle passing the GPU device into this container.
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        libgl1 libglib2.0-0 && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY worker.py .

# Download YOLO model weights at build time (not every container start)
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')"

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility

CMD ["python", "worker.py"]
