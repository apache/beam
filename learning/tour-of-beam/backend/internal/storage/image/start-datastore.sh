#!/usr/bin/env bash

# Check user environment variable
if [[ -z "${DATASTORE_PROJECT_ID}" ]]; then
  echo "Missing DATASTORE_PROJECT_ID environment variable" >&2
  exit 1
fi

if [[ -z "${DATASTORE_LISTEN_ADDRESS}" ]]; then
  echo "Missing DATASTORE_LISTEN_ADDRESS environment variable" >&2
  exit 1
fi

options=${options:1}
# Check for datastore options
while [ ! $# -eq 0 ]
do
  case "$1" in
    --store-on-disk)
      options="$options --store-on-disk"
      shift
      ;;
    --no-store-on-disk)
      options="$options --no-store-on-disk"
      shift
      ;;
    --consistency=*)
      consistency=${1#*=}
      options="$options --consistency=$consistency"
      shift
      ;;
    *)
      echo "Invalid option: $1. Use: --store-on-disk, --no-store-on-disk, --consistency=[0.0-1.0]"
      exit 1
      ;;
  esac
done

# Config gcloud project
gcloud config set project ${DATASTORE_PROJECT_ID}

# Start emulator
gcloud beta emulators datastore start \
  --data-dir=/opt/data \
  --host-port=${DATASTORE_LISTEN_ADDRESS} \
  ${options}
