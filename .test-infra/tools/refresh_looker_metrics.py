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
import requests
from google.cloud import storage

# Load environment variables
LOOKER_API_URL = os.getenv("LOOKERSDK_BASE_URL")
LOOKER_CLIENT_ID = os.getenv("LOOKERSDK_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKERSDK_CLIENT_SECRET")
TARGET_BUCKET = os.getenv("GCS_BUCKET")

# List of Look IDs to download
LOOKS_TO_DOWNLOAD = ["Dcvfh3XFZySrsmPY4Rm8NYyMg5QQRBF6", "nwQxvsnQFdBPTk27pZYxjcGNm2rRfNJk"]


def get_looker_token():
    """Authenticate with Looker API and return an access token."""
    url = f"{LOOKER_API_URL}/login"
    payload = {
        "client_id": LOOKER_CLIENT_ID,
        "client_secret": LOOKER_CLIENT_SECRET
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["access_token"]


def download_look(token, look_id):
    """Download Look as PNG."""
    url = f"{LOOKER_API_URL}/looks/{look_id}/run/png"
    headers = {"Authorization": f"token {token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to download Look {look_id}: {response.text}")
        return None


def upload_to_gcs(bucket_name, destination_blob_name, content):
    """Upload content to GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload content, overwriting if it exists
    blob.upload_from_string(content, content_type="image/png")
    print(f"Uploaded {destination_blob_name} to {bucket_name}.")


def main():
    token = get_looker_token()

    for look_id in LOOKS_TO_DOWNLOAD:
        if look_id:
            content = download_look(token, look_id)
            if content:
                upload_to_gcs(TARGET_BUCKET, f"{look_id}.png", content)


if __name__ == "__main__":
    main()
