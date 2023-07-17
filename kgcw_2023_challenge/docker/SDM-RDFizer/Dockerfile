################################################################################
# SDM-RDFizer 
# https://github.com/SDM-TIB/SDM-RDFizer
################################################################################
FROM ubuntu:22.04
# Configure the SDM-RDFizer version to use
ARG SDMRDFIZER_VERSION

# Install latest updates and dependencies
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y python3 python3-pip less vim wget unzip

# Download SDM-RDFizer release
RUN wget -O sdm-rdfizer.zip https://github.com/SDM-TIB/SDM-RDFizer/archive/refs/tags/v${SDMRDFIZER_VERSION}.zip
RUN unzip sdm-rdfizer.zip && mv SDM-RDFizer-* sdm-rdfizer 
RUN python3 -m pip install -r sdm-rdfizer/requirements.txt

# Expose gtfs-bench folders
RUN mkdir /data
RUN mkdir /mappings
RUN mkdir /results
RUN mkdir /configs

# Silent
CMD ["tail", "-f", "/dev/null"]
