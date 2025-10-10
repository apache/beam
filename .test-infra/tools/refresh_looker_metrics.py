# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import time
import looker_sdk

from google.cloud import storage
from looker_sdk import models40 as models

# Load environment variables
LOOKER_API_URL = os.getenv("LOOKERSDK_BASE_URL")
LOOKER_CLIENT_ID = os.getenv("LOOKERSDK_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKERSDK_CLIENT_SECRET")
TARGET_BUCKET = os.getenv("GCS_BUCKET")

# List of Pairs (Target folder name, Look IDs to download)
LOOKS_TO_DOWNLOAD = [
    ("30", ["18", "50", "92", "49", "91"]),    # BigQueryIO_Read
    ("31", ["19", "52", "88", "51", "87"]),    # BigQueryIO_Write
    ("32", ["20", "60", "104", "59", "103"]),  # BigTableIO_Read
    ("33", ["21", "70", "116", "69", "115"]),  # BigTableIO_Write
    ("34", ["22", "56", "96", "55", "95"]),    # TextIO_Read
    ("35", ["23", "64", "110", "63", "109"]),  # TextIO_Write
    ("75", ["258", "259", "260", "261", "262"]),  # TensorFlow MNIST
    ("76", ["233", "234", "235", "236", "237"]),  # PyTorch BERT base uncased
    ("77", ["238", "239", "240", "241", "242"]),  # PyTorch BERT large uncased
    ("78", ["243", "244", "245", "246", "247"]),  # PyTorch Resnet 101
    ("79", ["248", "249", "250", "251", "252"]),  # PyTorch Resnet 152
    ("80", ["253", "254", "255", "256", "257"]),  # PyTorch Resnet 152 Tesla T4
    ("82", ["263", "264", "265", "266", "267"]),  # PyTorch Sentiment Streaming DistilBERT base uncased
    ("85", ["268", "269", "270", "271", "272"]),  # PyTorch Sentiment Batch DistilBERT base uncased
    ("86", ["284", "285", "286", "287", "288"]),  # VLLM Batch Gemma
]


def get_look(id: str) -> models.Look:
    look = next(iter(sdk.search_looks(id=id)), None)
    if not look:
        raise Exception(f"look '{id}' was not found")
    print(f"Found look with public_slug = {look.public_slug}")
    return look


def download_look(look: models.Look):
    """Download specified look as png/jpg"""
    task = sdk.create_look_render_task(look.id, "png", 810, 526,)

    if not (task and task.id):
        raise Exception(
            f"Could not create a render task for '{look.title}'"
        )

    # poll the render task until it completes
    elapsed = 0.0
    delay = 20
    retries = 0
    max_retries = 20
    while retries < max_retries:
        poll = sdk.render_task(task.id)
        if poll.status == "failure":
            print(poll)
            raise Exception(f"Render failed for '{look.title}'")
        elif poll.status == "success":
            break
        time.sleep(delay)
        elapsed += delay
        retries += 1
        print(f"Retry {retries}/{max_retries}: Render task still in progress...")

    if retries >= max_retries:
        raise TimeoutError(f"Render task did not complete within {elapsed} seconds (max retries: {max_retries})")

    print(f"Render task completed in {elapsed} seconds")

    return sdk.render_task_results(task.id)


def upload_to_gcs(bucket_name, destination_blob_name, content):
    """Upload content to GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload content, overwriting if it exists
    blob.upload_from_string(content, content_type="image/png")
    print(f"Uploaded {destination_blob_name} to {bucket_name}.")


sdk = looker_sdk.init40()


def main():
    failed_looks = []

    for folder, look_ids in LOOKS_TO_DOWNLOAD:
        for look_id in look_ids:
            try:
                if look_id:
                    look = get_look(look_id)
                    content = download_look(look)
                    if content:
                        upload_to_gcs(TARGET_BUCKET, f"{folder}/{look.public_slug}.png", content)
                    else:
                        print(f"No content for look {look_id}")
                        failed_looks.append(look_id)
            except Exception as e:
                print(f"Error processing look {look_id}: {e}")
                failed_looks.append(look_id)

    if failed_looks:
        raise RuntimeError(f"Job failed due to errors in looks: {failed_looks}")


if __name__ == "__main__":
    main()
