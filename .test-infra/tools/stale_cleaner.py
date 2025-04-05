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
import pickle
from collections import defaultdict
from google.cloud import pubsub_v1, storage

# Resource types
PUBSUB_TOPIC_RESOURCE = "pubsub_topic"

# Storage constants
STORAGE_PREFIX = "stale_cleaner/"
WORKING_SUFFIX = "-working"
DELETED_SUFFIX = "-deleted"

# File paths
WORKING_RESOURCES_TEMP_FILE = "/tmp/working_resources.bin"
LAST_WORKING_RESOURCES_TEMP_FILE = "/tmp/last_working_resources.bin"
DELETED_RESOURCES_TEMP_FILE = "/tmp/deleted_resources.bin"
LAST_DELETED_RESOURCES_TEMP_FILE = "/tmp/last_deleted_resources.bin"

# Project constants
PROJECT_PATH_PREFIX = "projects/"

# Time constants (in seconds)
DEFAULT_PUBSUB_TOPIC_THRESHOLD = 86400  # 1 day
DEFAULT_TIME_THRESHOLD = 3600  # 1 hour

# Default values for testing
DEFAULT_PROJECT_ID = "apache-beam-testing"
DEFAULT_BUCKET_NAME = "apache-beam-testing-pabloem"

class GoogleCloudResource:
    resource_name = ""
    resource_type = ""
    creation_date = datetime.datetime.now()
    last_update_date = datetime.datetime.now()

    def __init__(self, resource_name, resource_type, creation_date=datetime.datetime.now(), last_update_date=datetime.datetime.now()) -> None:
        self.resource_name = resource_name
        self.resource_type = resource_type
        self.creation_date = creation_date
        self.last_update = last_update_date

    def __str__(self) -> str:
        return f"{self.resource_name}"

    def update(self) -> None:
        self.last_update_date = datetime.datetime.now()

    def time_alive(self) -> datetime.timedelta:
        return datetime.datetime.now() - self.creation_date

class StaleCleaner:
    """
    StaleCleaner is a class that is used to detect stale resources in the Google Cloud Platform.
    It is used to detect resources that are no longer needed and delete them.

    The update process goes through the following steps:

    1. Get the resources that exist right now
    2. Get the resources that were working the last time this script was run
    3. Get the resources that were already deleted the last time this script was run
    4. Go through the working resource list comparing the working list with the new list
        5. If no longer exists, add it to the deleted dictionary
        6. If it exists, update the last update date
        7. If the resource did not existed before, add it to the working dictionary
    8. Delete the resources that are no longer alive (Not implemented yet)
    9. Add all the new resources to the working dictionary
    10. Save the working and deleted resources to the google bucket
    """

    # Create a new StaleCleaner object
    def __init__(self, project_id, bucket_name, prefixes=None, time_threshold=DEFAULT_TIME_THRESHOLD) -> None:
        self.project_id = project_id
        self.project_path = f"{PROJECT_PATH_PREFIX}{project_id}"
        self.bucket_name = bucket_name
        self.prefixes = prefixes or []
        self.time_threshold = time_threshold

    # Dictionary of resources that exists right now, this needs to be created for each resource type
    def get_now_resources(self) -> defaultdict:
        pass

    # Get working dictionary of resources from google bucket
    def get_working_resources(self) -> defaultdict:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=STORAGE_PREFIX)

        working_filtered_blobs = [blob for blob in blobs if f"{self.resource_type}{WORKING_SUFFIX}" in blob.name]
        if working_filtered_blobs:
            last_working_blob = max(working_filtered_blobs, key=lambda b: b.time_created)
        else :
            last_working_blob = None

        # Get the resource dictionary from the blob, if it exists, else create an empty dictionary
        if last_working_blob is not None:
            # Download the last resources from the blob
            last_working_blob.download_to_filename(LAST_WORKING_RESOURCES_TEMP_FILE)
            with open(LAST_WORKING_RESOURCES_TEMP_FILE, "rb") as f:
                # Load the last resources from the blob
                working_resource_dict = pickle.load(f)
        else:
            working_resource_dict = defaultdict(GoogleCloudResource)

        return working_resource_dict

    # Get deleted resources from google bucket
    def get_deleted_resources(self) -> defaultdict:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=STORAGE_PREFIX)

        # Get the deleted resources dictionary from the blob, if it exists, else create an empty dictionary
        deleted_filtered_blobs = [blob for blob in blobs if f"{self.resource_type}{DELETED_SUFFIX}" in blob.name]
        if deleted_filtered_blobs:
            last_deleted_blob = max(deleted_filtered_blobs, key=lambda b: b.time_created)
        else:
            last_deleted_blob = None

        # Get the resource dictionary from the blob, if it exists, else create an empty dictionary
        if last_deleted_blob is not None:
            # Download the last resources from the blob
            last_deleted_blob.download_to_filename(LAST_DELETED_RESOURCES_TEMP_FILE)
            with open(LAST_DELETED_RESOURCES_TEMP_FILE, "rb") as f:
                # Load the last resources from the blob
                deleted_resource_dict = pickle.load(f)
        else:
            deleted_resource_dict = defaultdict(GoogleCloudResource)

        return deleted_resource_dict

    # Get the resources that are older than the threshold (in seconds)
    def get_old_resources(self) -> defaultdict:
        # Traverse the working resources and get the ones that are older than the threshold
        working_resource_dict = self.get_working_resources()
        old_resource_dict = defaultdict(GoogleCloudResource)

        time_threshold_delta = datetime.timedelta(seconds=self.time_threshold)
        for resource_name, resource_obj in working_resource_dict.items():
            if resource_obj.time_alive() > time_threshold_delta:
                old_resource_dict[resource_name] = resource_obj

        return old_resource_dict

    # Set the working dictionary of resources in the google bucket
    def set_working_resources(self, working_resource_dict) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)

        with open(WORKING_RESOURCES_TEMP_FILE, "wb") as f:
            pickle.dump(working_resource_dict, f)
        blob = bucket.blob(f"{STORAGE_PREFIX}{self.resource_type}{WORKING_SUFFIX}")
        blob.upload_from_filename(WORKING_RESOURCES_TEMP_FILE)

    # Set the deleted dictionary of resources in the google bucket
    def set_deleted_resources(self, deleted_resource_dict) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)

        with open(DELETED_RESOURCES_TEMP_FILE, "wb") as f:
            pickle.dump(deleted_resource_dict, f)
        blob = bucket.blob(f"{STORAGE_PREFIX}{self.resource_type}{DELETED_SUFFIX}")
        blob.upload_from_filename(DELETED_RESOURCES_TEMP_FILE)

    # Save the new project state
    def update(self) -> None:
        # Get working resource list
        now_resources_dict = self.get_now_resources()
        working_resource_dict = self.get_working_resources()
        deleted_resource_dict = self.get_deleted_resources()

        working_resources_to_delete = []

        # Go through the working resource list comparing the working list with the new list
        for resource_name, resource_obj in working_resource_dict.items():
            # If no longer exists, add it to the deleted dictionary
            if resource_name not in now_resources_dict:
                deleted_resource_dict[resource_name] = resource_obj
                working_resources_to_delete.append(resource_name)
            # If it exists, update the last update date
            else:
                working_resource_dict[resource_name].update()
                now_resources_dict.pop(resource_name)

        # Delete the resources that are no longer alive
        for resource_name in working_resources_to_delete:
            working_resource_dict.pop(resource_name)

        # Add all the new resources to the working dictionary
        for resource_name, resource_obj in now_resources_dict.items():
            working_resource_dict[resource_name] = resource_obj

        # Save the working and deleted resources
        self.set_working_resources(working_resource_dict)
        self.set_deleted_resources(deleted_resource_dict)

# Create a new PubSub topic cleaner
class PubSubTopicCleaner(StaleCleaner):
    def __init__(self, project_id, bucket_name, prefixes=None, time_threshold=DEFAULT_PUBSUB_TOPIC_THRESHOLD) -> None:
        super().__init__(project_id, bucket_name, prefixes, time_threshold)
        self.client = pubsub_v1.PublisherClient()
        self.resource_type = PUBSUB_TOPIC_RESOURCE

    def get_now_resources(self) -> defaultdict:
        d = defaultdict(GoogleCloudResource)
        for topic in self.client.list_topics(request={"project": self.project_path}):
            # Only include topics that match any of the specified prefixes
            topic_name = topic.name.split('/')[-1]
            if not self.prefixes or any(topic_name.startswith(prefix) for prefix in self.prefixes):
                d[topic.name] = GoogleCloudResource(topic.name, self.resource_type)
        return d

# Local testing
if __name__ == "__main__":
    cleaner = PubSubTopicCleaner(DEFAULT_PROJECT_ID, DEFAULT_BUCKET_NAME)
    cleaner.update()

    print("Resources that exist right now")
    d = cleaner.get_now_resources()
    for k, v in d.items():
        print(f"{k} -> {v.time_alive()}")

    print("Resources that were working the last time this script was run")
    d = cleaner.get_working_resources()
    for k, v in d.items():
        print(f"{k} -> {v.time_alive()}")

    print("Resources that were already deleted the last time this script was run")
    d = cleaner.get_deleted_resources()
    for k, v in d.items():
        print(f"{k} -> {v.time_alive()}")

    print(f"Resources that are older than {cleaner.time_threshold} seconds")
    d = cleaner.get_old_resources()
    for k, v in d.items():
        print(f"{k} -> {v.time_alive()}")

