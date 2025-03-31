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
import requests
from google.cloud import storage

from looker_sdk import models40 as models
from looker_sdk.sdk.api40.methods import Looker40SDK
from looker_sdk.rtl import transport, auth_session, requests_transport

# Load environment variables
LOOKER_API_URL = os.getenv("LOOKERSDK_BASE_URL")
LOOKER_CLIENT_ID = os.getenv("LOOKERSDK_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKERSDK_CLIENT_SECRET")
TARGET_BUCKET = os.getenv("GCS_BUCKET")

# List of Pairs (Target folder name, Look IDs to download)
LOOKS_TO_DOWNLOAD = [
    ("30", ["18", "50"]),    # BigQueryIO_Read
]


def get_access_token():
    """Retrieve OAuth2 access token using client credentials"""
    token_url = f"{LOOKER_API_URL}/login"
    response = requests.post(token_url, data={
        'client_id': LOOKER_CLIENT_ID,
        'client_secret': LOOKER_CLIENT_SECRET
    })
    response.raise_for_status()
    return response.json()['access_token']


class MyAuthSession(auth_session.AuthSession):
    def __init__(self, token: str, base_url: str):
        self._token = token
        self._base_url = base_url

    def get_token(self):
        return self._token

    def is_authenticated(self) -> bool:
        return True

    def authenticate(self):
        return self._token

    def logout(self):
        pass

    def is_anonymous(self):
        return False


def init_sdk():
    token = get_access_token()
    transport_options = requests_transport.RequestsTransportOptions(
        verify=True
    )
    transporter = requests_transport.RequestsTransport(
        options=transport_options,
        base_url=LOOKER_API_URL,
        auth=MyAuthSession(token, LOOKER_API_URL)
    )
    return Looker40SDK(transporter)


sdk = init_sdk()


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
