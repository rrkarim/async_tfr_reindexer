# Use the official Ubuntu 20.04 LTS base image
FROM ubuntu:22.04

# Set a default value for the DEBIAN_FRONTEND variable
# This ensures that package installation doesn't try to prompt for input
ENV DEBIAN_FRONTEND=noninteractive

# Update the package repository and install build-essential
# Also, clean up the cache to reduce the size of the created image
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    python3-dev \
    python3 \
    python3-pip \
    python3-venv \
    git \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# FIXME: write instructions on how to prepare .local dir
COPY .local /root/.local
ENV MY_INSTALL_DIR=/root/.local


# Create a Python virtual environment
RUN python3 -m venv /opt/venv

# Activate the virtual environment and install pybind11
RUN . /opt/venv/bin/activate && \
    pip install --upgrade pip && \
    pip install pybind11 pybind11[global] tqdm \
    google-cloud \
    google-cloud-storage \
    aiohttp \
    gcloud-aio-storage \
    xxhash

# [Optional] Set a default command for the container
# This is just an example; you can change it to whatever makes sense for your use case
CMD ["/bin/bash"]