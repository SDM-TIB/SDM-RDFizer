# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory to /app for installing rdfizer
WORKDIR /app

# Copy the requirements.txt alone to re-install packages only if it has changed
ADD requirements.txt /app

RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    gcc \
    g++ \
    gnupg \
    curl \
    && rm -rf /var/lib/apt/lists/*


# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

# Add all the source code
ADD . /app

# Fix issue with symlinks not being copied in the docker image:
ADD README.md VERSION requirements.txt /app/rdfizer/

# Install the rdfizer package
RUN pip install ./rdfizer

# Set the working directory to /data
WORKDIR /data

# Use the rdfizer package as entrypoint
ENTRYPOINT [ "rdfizer" ]

# Default args passed to the entrypoint
# Run config.ini in workdir /data by default
CMD ["-c", "config.ini"]

## Usage:
# docker build -t rdfizer:latest -f Dockerfile.cli .
# docker run -it --rm -v $(pwd)/example:/data rdfizer:latest -c config.ini