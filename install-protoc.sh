#!/bin/sh
# See more: https://github.com/protocolbuffers/protobuf/releases/tag/v3.13.0
apt-get update -y && \
    apt-get install -y unzip && \
    wget https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protoc-3.13.0-linux-x86_64.zip && \
    unzip protoc-3.13.0-linux-x86_64.zip -d /usr/local/ && \
    rm protoc-3.13.0-linux-x86_64.zip 
