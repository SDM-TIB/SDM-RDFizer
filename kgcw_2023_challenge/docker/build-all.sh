#!/bin/sh
set -e
#
# Build script for all Docker containers.
#
# Copyright (c) by ANONYMOUS (2022)
# License: GPLv3
#

MYSQL_VERSION='8.0'
POSTGRESQL_VERSION='14.5'
VIRTUOSO_VERSION='7.2.7'
FUSEKI_VERSION='4.6.1'
MORPHKGC_VERSION='2.2.0'
MORPHRDB_VERSION='3.12.5'
ONTOP_VERSION='5.0.0'
RMLMAPPER_VERSION='6.0.0'
RMLMAPPER_BUILD='363'
SDMRDFIZER_VERSION='4.7.1.2'
YARRRML_VERSION='1.3.6'

# MySQL
echo "*** Building MySQL $MYSQL_VERSION ... ***"
cd MySQL
docker build --build-arg MYSQL_VERSION=$MYSQL_VERSION \
    -t blindreviewing/mysql:v$MYSQL_VERSION .
#docker push blindreviewing/mysql:v$MYSQL_VERSION
cd ..

# Virtuoso
echo "*** Building Virtuoso $VIRTUOSO_VERSION ... ***"
cd Virtuoso
docker build --build-arg VIRTUOSO_VERSION=$VIRTUOSO_VERSION \
    -t blindreviewing/virtuoso:v$VIRTUOSO_VERSION .
#docker push blindreviewing/virtuoso:v$VIRTUOSO_VERSION
cd ..

# SDM-RDFizer
echo "*** Building SDM-RDFizer $SDMRDFIZER_VERSION ... ***"
cd SDM-RDFizer
docker build --build-arg SDMRDFIZER_VERSION=$SDMRDFIZER_VERSION \
    -t blindreviewing/sdm-rdfizer:v$SDMRDFIZER_VERSION .
#docker push blindreviewing/sdm-rdfizer:v$SDMRDFIZER_VERSION
cd ..
