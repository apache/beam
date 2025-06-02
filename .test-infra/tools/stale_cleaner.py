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

# Resource types
PUBSUB_TOPIC_RESOURCE = "pubsub_topic"

# Storage constants
STORAGE_PREFIX = "stale_cleaner/"

# Project constants
PROJECT_PATH_PREFIX = "projects/" # Prefix for the project path in GCP *This is not the project id*

# Time constants (in seconds)
DEFAULT_PUBSUB_TOPIC_THRESHOLD = 86400  # 1 day
DEFAULT_TIME_THRESHOLD = 3600  # 1 hour

# Default values for testing
DEFAULT_PROJECT_ID = "apache-beam-testing"
DEFAULT_BUCKET_NAME = "apache-beam-testing-pabloem"

class Clock:
    """
    Clock is an abstract class that returns the current datetime.
    It is used to get the current time in the application.
    """
    def __call__(self) -> datetime.datetime:
        raise NotImplementedError("Subclasses must implement __call__ method")

class RealClock(Clock):
    """
    RealClock is a class that returns the current datetime.
    """
    def __call__(self) -> datetime.datetime:
        return datetime.datetime.now()

class FakeClock(Clock):
    """
    FakeClock is a class that returns a fixed datetime.
    It is used for testing purposes.
    """
    def __init__(self, datetime_str: str) -> None:
        self.clock = datetime.datetime.fromisoformat(datetime_str)

    def __call__(self) -> datetime.datetime:
        return self.clock

    def set(self, datetime_str: str) -> None:
        """
        Set the clock to a specific datetime.
        """
        self.clock = datetime.datetime.fromisoformat(datetime_str)

class GoogleCloudResource:
    """
    GoogleCloudResource is a class used to store the GCP resource information of name and type
    including the creation date and last check date.
    """
    def __init__(self, resource_name: str, creation_date: datetime.datetime = None,
                    last_update_date: datetime.datetime = None, clock: Clock = None) -> None:
        self.resource_name = resource_name
        effective_clock = clock or RealClock() # Use provided clock or RealClock
        current_time = effective_clock()
        self.creation_date = creation_date or current_time # Date of first appearance of the resource
        self.last_update_date = last_update_date or current_time # Date of last existence check

    def __str__(self) -> str:
        return f"{self.resource_name}"

    def to_dict(self) -> dict:
        """
        Convert the resource to a dictionary.
        """
        return {
            "resource_name": self.resource_name,
            "creation_date": self.creation_date.isoformat(),
            "last_update_date": self.last_update_date.isoformat()
        }

    def update(self, clock: Clock = None) -> None:
        effective_clock = clock or RealClock()
        self.last_update_date = effective_clock()

    def time_alive(self, clock: Clock = None) -> int:
        """
        Get the time since the resource was created (in seconds).
        """
        effective_clock = clock or RealClock()
        return (effective_clock() - self.creation_date).total_seconds()

class StaleCleaner:
    """
    StaleCleaner is a class that is used to detect stale resources in the Google Cloud Platform.
    It is used to detect resources that are no longer needed and delete them.

    Methods:

    refresh():
        Load all data with the current datetime

    stale_resources():
        Dict of _stale_ resources that should be deleted

    fresh_resources():
        Dict of resources that are NOT stale

    def delete_stale(dry_run=True):
        Delete all stale resources (dry_run by default)
    """

    # Create a new StaleCleaner object
    def __init__(self, project_id: str, resource_type: str, bucket_name: str,
                    prefixes: list = None, time_threshold: int = DEFAULT_TIME_THRESHOLD,
                    clock: Clock = None) -> None:
        self.project_id = project_id
        self.project_path = f"{PROJECT_PATH_PREFIX}{project_id}"
        self.resource_type = resource_type
        self.bucket_name = bucket_name
        self.prefixes = prefixes or []
        self.time_threshold = time_threshold
        self.clock = clock or RealClock()

    def _delete_resource(self, resource_name: str) -> None:
        """
        Different for each resource type. Delete the resource from GCP.
        """
        pass

    def _active_resources(self) -> dict:
        """
        Different for each resource type. Get the active resources from GCP as a dictionary.
        The dictionary is a dict of GoogleCloudResource objects.
        The key is the resource name and the value is the GoogleCloudResource object.
        The clock is for testing purposes. It gives the resources a specific creation date.
        """
        pass

    def _write_resources(self, resources: dict) -> None:
        """
        Write existing resources to the google bucket.
        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"{STORAGE_PREFIX}{self.resource_type}.json")

        resource_dict = {k: v.to_dict() for k, v in resources.items()}
        blob_json = json.dumps(resource_dict, indent=4)

        blob.upload_from_string(blob_json, content_type="application/json")
        print(f"Resources written to {self.bucket_name}/{STORAGE_PREFIX}{self.resource_type}.json")

    def _stored_resources(self) -> dict:
        """
        Get the stored resources from the google bucket.
        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"{STORAGE_PREFIX}{self.resource_type}.json")

        if not blob.exists():
            print(f"Blob {self.bucket_name}/{STORAGE_PREFIX}{self.resource_type}.json does not exist.")
            return {}

        blob_string = blob.download_as_text()
        blob_dict = json.loads(blob_string)

        # Convert the dictionary to a dict of GoogleCloudResource objects
        resources = {}
        for k, v in blob_dict.items():
            resources[k] = GoogleCloudResource(
                resource_name=v["resource_name"],
                creation_date=datetime.datetime.fromisoformat(v["creation_date"]),
                last_update_date=datetime.datetime.fromisoformat(v["last_update_date"]),
                clock=self.clock
            )
        return resources

    def refresh(self) -> None:
        """
        Refresh the resources time and save them to the google bucket.
        The process goes through the following steps:
            1. Get the resources that exist in the GCP
            2. Get the resources that were working the last time this script was run
            3. Delete from the stored resources the ones that are no longer alive
            4. Add the new resources to the working dictionary
            5. Save the working resources to the google bucket
        """
        stored_resources = self._stored_resources()
        active_resources = self._active_resources()

        for k, v in list(stored_resources.items()):
            if k not in active_resources:
                print(f"Resource {k} is no longer alive. Deleting it from the stored resources.")
                del stored_resources[k]
            else:
                v.update(clock=self.clock)


        for k, v in active_resources.items():
            if k not in stored_resources:
                stored_resources[k] = v

        self._write_resources(stored_resources)

    def stale_resources(self) -> dict:
        """
        Get the stale resources that should be deleted.
        The process goes through the following steps:
            1. Get the stored resources
            2. Compare the time since the creation date of the resource with the time threshold
            3. If the time since the creation date is greater than the time threshold, add it to the stale resources
        """
        stored_resources = self._stored_resources()
        stale_resources = {}

        for k, v in stored_resources.items():
            if v.time_alive(clock=self.clock) > self.time_threshold:
                stale_resources[k] = v

        return stale_resources

    def fresh_resources(self) -> dict:
        """
        Get the fresh resources that are not stale.
        The process goes through the following steps:
            1. Get the stored resources
            2. Compare the time since the creation date of the resource with the time threshold
            3. If the time since the creation date is less than the time threshold, add it to the fresh resources
        """
        stored_resources = self._stored_resources()
        fresh_resources = {}

        for k, v in stored_resources.items():
            if v.time_alive(clock=self.clock) <= self.time_threshold:
                fresh_resources[k] = v

        return fresh_resources

    def delete_stale(self, dry_run: bool = True) -> None:
        """
        Delete the stale resources.
        The process goes through the following steps:
            1. Get the stale resources
            2. Check if they still exist in GCP
            3. If they exist, delete them
            4. If dry_run is True, do not delete them, just print the names
        """
        stale_resources_map = self.stale_resources()
        active_resources_map = self._active_resources()

        for k, v in stale_resources_map.items():
            if k in active_resources_map:
                if dry_run:
                    print(f"Dry run: Would delete resource {k}")
                else:
                    print(f"Deleting resource {k}")
                    self._delete_resource(k)
            else:
                print(f"Resource {k} marked as stale but no longer exists in GCP. Skipping deletion.")

        if not dry_run:
            self.refresh()


# PubSub topic cleaner
class PubSubTopicCleaner(StaleCleaner):
    def __init__(self, project_id: str, bucket_name: str,
                    prefixes: list = None, time_threshold: int = DEFAULT_PUBSUB_TOPIC_THRESHOLD,
                    clock: Clock = None) -> None:
        super().__init__(project_id, PUBSUB_TOPIC_RESOURCE, bucket_name, prefixes, time_threshold, clock)
        self.client = pubsub_v1.PublisherClient()

    def _active_resources(self) -> dict:
        d = {}
        for topic in self.client.list_topics(request={"project": self.project_path}):
            topic_name = topic.name
            # Apply prefix filtering if prefixes are defined
            if not self.prefixes or any(topic_name.startswith(f"{self.project_path}/topics/{prefix}") for prefix in self.prefixes):
                d[topic_name] = GoogleCloudResource(resource_name=topic_name, clock=self.clock)
        return d

    def _delete_resource(self, resource_name: str) -> None:
        topic_name = resource_name.split('/')[-1]
        print(f"Deleting PubSub topic {topic_name}")
        self.client.delete_topic(name=resource_name)
