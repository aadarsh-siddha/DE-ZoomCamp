#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

TAXI_TYPE=$1 # e.g., "yellow"
YEAR=$2 # e.g., 2020

URL_PREFIX='https://d37ci6vzurychx.cloudfront.net/trip-data'

for MONTH in {1..12}; do
    FMONTH=$(printf "%02d" ${MONTH})
    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
    
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
    
    mkdir -p "${LOCAL_PREFIX}"
    wget "${URL}" -O "${LOCAL_PATH}"
done
