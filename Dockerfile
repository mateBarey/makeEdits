# Use an official Python runtime as a parent image
FROM python:3.10.13-slim-bookworm

# Set the working directory in the container
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install system dependencies required for TA-Lib
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget

# Download and install TA-Lib C library with conditional architecture handling
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xvzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib/ \
    && if [ "$(uname -m)" = "x86_64" ]; then \
         ./configure --build=x86_64-unknown-linux-gnu --prefix=/usr; \
       elif [ "$(uname -m)" = "aarch64" ]; then \
         ./configure --build=aarch64-unknown-linux-gnu --prefix=/usr; \
       else \
         ./configure --prefix=/usr; \
       fi \
    && make \
    && make install \
    && cd .. \
    && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# After installing TA-Lib C library, install ta-lib Python package
RUN pip install ta-lib

# Cleanup
RUN apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
       build-essential wget \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY app/ . 

RUN ls -la /app > dir_contents.txt 

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define environment variable
ENV NAME World

# Run uvicorn when the container launches
CMD ["uvicorn", "fast_api_mgr_onnx:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "uvloop"]
