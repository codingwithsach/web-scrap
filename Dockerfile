# Use Python 3.11 base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ARCHIVER_DATA_PATH=/data/current_events \
    MAX_WORKERS=3 \
    REQUEST_DELAY=2 \
    MAX_ARTICLES=10 \
    ENABLE_AUDIO=true \
    ENABLE_VIDEO=true \
    MAX_MEDIA_SIZE_MB=1024 \
    WKHTMLTOPDF_PATH=/usr/local/bin/wkhtmltopdf \
    ENABLE_TTS=true \
    TTS_LANGUAGE=en \
    TTS_SPEED=1.0 \
    TTS_CHUNK_SIZE=4000 \
    TTS_MAX_LENGTH=50000 \
    TTS_AUDIO_FORMAT=mp3 \
    TTS_BITRATE=64k \
    SAVE_TEXT_FILE=true \
    TEXT_FORMAT=both

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    xz-utils \
    fontconfig \
    libfreetype6 \
    libjpeg62-turbo \
    libpng16-16 \
    libx11-6 \
    libxext6 \
    libxrender1 \
    libssl3 \
    ca-certificates \
    fonts-liberation \
    fonts-dejavu \
    # Audio processing dependencies
    ffmpeg \
    libavcodec-extra \
    portaudio19-dev \
    libasound2-dev \
    # Additional dependencies for image processing
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libxml2-dev \
    libxslt-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install wkhtmltopdf
RUN wget -q https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6.1-3/wkhtmltox_0.12.6.1-3.bookworm_amd64.deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends ./wkhtmltox_0.12.6.1-3.bookworm_amd64.deb \
    && rm wkhtmltox_0.12.6.1-3.bookworm_amd64.deb \
    && ln -s /usr/local/bin/wkhtmltopdf /usr/bin/wkhtmltopdf \
    && wkhtmltopdf --version \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source code
COPY . .

# Create a non-root user
RUN useradd -u 1000 -m appuser

# Create necessary directories with proper permissions
RUN mkdir -p /data/current_events \
    /data/current_events/logs \
    /data/current_events/media \
    /data/current_events/temp \
    /tmp/wkhtmltopdf

# Set proper ownership
RUN chown -R appuser:appuser /app /data /tmp/wkhtmltopdf

# Switch to the non-root user
USER appuser

# Run the app
CMD ["python", "archiver.py"]