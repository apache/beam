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
    # Create a new StaleCleaner object
    def __init__(self, project_id) -> None:
        self.project_id = project_id
        self.project_path = f"projects/{project_id}"

    # Dictionary of resources that exists right now, this needs to be created for each resource type
    def get_now_resources(self) -> defaultdict:
        pass

    # Get working dictionary of resources from google bucket
    def get_working_resources(self, bucket_name) -> defaultdict:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="stale_cleaner/")

        working_filtered_blobs = [blob for blob in blobs if f"{self.resource_type}-working" in blob.name]
        if working_filtered_blobs:
            last_working_blob = max(working_filtered_blobs, key=lambda b: b.time_created)
        else :
            last_working_blob = None

        # Get the resource dictionary from the blob, if it exists, else create an empty dictionary
        if last_working_blob is not None:
            # Download the last resources from the blob
            last_working_blob.download_to_filename("/tmp/last_working_resources.bin")
            with open("/tmp/last_working_resources.bin", "rb") as f:
                # Load the last resources from the blob
                working_resource_dict = pickle.load(f)
        else:
            working_resource_dict = defaultdict(GoogleCloudResource)

        return working_resource_dict

    def get_deleted_resources(self, bucket_name) -> defaultdict:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="stale_cleaner/")

        # Get the deleted resources dictionary from the blob, if it exists, else create an empty dictionary
        deleted_filtered_blobs = [blob for blob in blobs if f"{self.resource_type}-deleted" in blob.name]
        if deleted_filtered_blobs:
            last_deleted_blob = max(deleted_filtered_blobs, key=lambda b: b.time_created)
        else:
            last_deleted_blob = None

        # Get the resource dictionary from the blob, if it exists, else create an empty dictionary
        if last_deleted_blob is not None:
            # Download the last resources from the blob
            last_deleted_blob.download_to_filename("/tmp/last_deleted_resources.bin")
            with open("/tmp/last_deleted_resources.bin", "rb") as f:
                # Load the last resources from the blob
                deleted_resource_dict = pickle.load(f)
        else:
            deleted_resource_dict = defaultdict(GoogleCloudResource)

        return deleted_resource_dict

    # Get the resources that are older than the threshold (in seconds)
    def get_old_resources(self, bucket_name, time_threshold) -> defaultdict:
        # Traverse the working resources and get the ones that are older than the threshold
        working_resource_dict = self.get_working_resources(bucket_name)
        old_resource_dict = defaultdict(GoogleCloudResource)

        time_threshold = datetime.timedelta(seconds=time_threshold)
        for resource_name, resource_obj in working_resource_dict.items():
            if resource_obj.time_alive() > time_threshold:
                old_resource_dict[resource_name] = resource_obj

        return old_resource_dict

    # Set the working dictionary of resources in the google bucket
    def set_working_resources(self, working_resource_dict, bucket_name) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        with open("/tmp/working_resources.bin", "wb") as f:
            pickle.dump(working_resource_dict, f)
        blob = bucket.blob(f"stale_cleaner/{self.resource_type}-working")
        blob.upload_from_filename("/tmp/working_resources.bin")

    # Set the deleted dictionary of resources in the google bucket
    def set_deleted_resources(self, deleted_resource_dict, bucket_name) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        with open("/tmp/deleted_resources.bin", "wb") as f:
            pickle.dump(deleted_resource_dict, f)
        blob = bucket.blob(f"stale_cleaner/{self.resource_type}-deleted")
        blob.upload_from_filename("/tmp/deleted_resources.bin")

    # Save the new project state
    def update(self, bucket_name) -> None:
        # Get working resource list
        now_resources_dict = self.get_now_resources()
        working_resource_dict = self.get_working_resources(bucket_name)
        deleted_resource_dict = self.get_deleted_resources(bucket_name)

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
        self.set_working_resources(working_resource_dict, bucket_name)
        self.set_deleted_resources(deleted_resource_dict, bucket_name)

# Create a new PubSubCleaner object
class PubSubCleaner(StaleCleaner):
    def __init__(self, project_id) -> None:
        super().__init__(project_id)
        self.client = pubsub_v1.PublisherClient()
        self.resource_type = "pubsub"

    def get_now_resources(self) -> defaultdict:
        d = defaultdict(GoogleCloudResource)
        for topic in self.client.list_topics(request={"project": self.project_path}):
            d[topic.name] = GoogleCloudResource(topic.name, self.resource_type)
        return d

# Local testing
if __name__ == "__main__":
    cleaner = PubSubCleaner("apache-beam-testing")
    cleaner.update("apache-beam-testing-pabloem")

    print("Now resources")
    d = cleaner.get_now_resources()
    for k, v in d.items():
        print(f"{k} -> {v.time_alive()}")

    print("Working resources")
    d = cleaner.get_working_resources("apache-beam-testing-pabloem")
    for k, v in d.items():
        print(f"{k} -> {v.time_alive()}")

    print("Deleted resources")
    d = cleaner.get_deleted_resources("apache-beam-testing-pabloem")
    for k, v in d.items():
        print(f"{k} -> {v.time_alive()}")

    print("Old resources")
    d = cleaner.get_old_resources("apache-beam-testing-pabloem", 60)
    for k, v in d.items():
        print(f"{k} -> {v.time_alive()}")

