#!/usr/bin/env python
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Deletes stale and old resources from the Google Cloud Platform.
#    In order to detect them, save the current resources state and compare it
#    with a previous one. They are stored in a bucket in the Google Cloud Storage.
#
import datetime
import json
from google.cloud import pubsub_v1, storage

class StaleCleaner:
    # Create a new StaleCleaner object
    def __init__(self, project_id) -> None:
        self.project_id = project_id
        self.project_path = f"projects/{project_id}"

    # List the resources that are going to be deleted
    def list_resources(self) -> list:
        pass

    # Save the current state of the resources to the Google Cloud Storage bucket
    # blob's name is the resource type and the current timestamp
    def save(self, bucket_name) -> None:
        resource_list = self.list_resources()
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"stale_cleaner/{self.resource_type}-{datetime.datetime.now().isoformat()}.json")
        blob.upload_from_string(json.dumps(resource_list, indent=2), content_type="application/json")


    # List of resources that existed in the previous state as well as in the current state
    def list_resources_old(self, bucket_name) -> list:
        # Get the last blob from the bucket with the same resource type
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="stale_cleaner/")
        filtered_blobs = [blob for blob in blobs if self.resource_type in blob.name]
        if filtered_blobs:
            last_blob = max(filtered_blobs, key=lambda b: b.time_created)
        else:
            last_blob = None

        # If there is no previous state, return an empty list
        if last_blob is None:
            return []
        else:
            print(f"Last blob: {last_blob.name}")

        # Get the current state of the resources
        current_resources = self.list_resources()

        # Get the previous state of the resources
        last_blob.download_to_filename("/tmp/last_resources.json")

        with open("/tmp/last_resources.json", "rb") as f:
            last_resources = json.load(f)

        # Return the list of resources that existed in the previous state as well as in the current state
        return list(set(last_resources) & set(current_resources))

# Create a new PubSubCleaner object
class PubSubCleaner(StaleCleaner):
    def __init__(self, project_id) -> None:
        super().__init__(project_id)
        self.client = pubsub_v1.PublisherClient()
        self.resource_type = "pubsub"

    def list_resources(self) -> list:
        l = []
        for topic in self.client.list_topics(request={"project": self.project_path}):
            l.append(topic.name)
        return l

# Local testing
if __name__ == "__main__":
    cleaner = PubSubCleaner("apache-beam-testing")
    cleaner.save("apache-beam-testing-pabloem")
    cleaner.list_resources_old("apache-beam-testing-pabloem")