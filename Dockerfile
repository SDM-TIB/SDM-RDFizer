# Use an official Python runtime as a parent image
FROM python:3.5

# Set the working directory to /app
WORKDIR /data

# Copy the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN cd /app && pip3 install --trusted-host pypi.python.org -r requirements.txt && cd /data

# Make port 80 available to the world outside this container
EXPOSE 4000

# Define environment variable
ENV NAME RDFizer

# Run app.py when the container launches
CMD ["python3", "/app/app.py"]
