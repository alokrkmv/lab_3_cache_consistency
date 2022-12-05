#!/bin/sh
docker pull mongo
docker run -d -p 27017:27017 --name mongo_container mongo


