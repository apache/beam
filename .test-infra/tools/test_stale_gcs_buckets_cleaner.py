#!/usr/bin/env python
#
#    Licensed to the Apache Software Foundation (ASF) under one
#    or more contributor license agreements.  See the NOTICE file
#    distributed with this work for additional information
#    regarding copyright ownership.  The ASF licenses this file
#    to you under the Apache License, Version 2.0 (the
#    "License"); you may not use this file except in compliance
#    with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing,
#    software distributed under the License is distributed on an
#    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#    KIND, either express or implied.  See the License for the
#    specific language governing permissions and limitations
#    under the License.
#

import datetime
import unittest
from unittest import mock

import stale_gcs_buckets_cleaner as cleaner


UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 9, 30, 12, tzinfo=UTC)
STALE_BUCKET = "apache-beam-temp-bucket-123e4567-e89b-42d3-a456-426614174000"
FRESH_BUCKET = "apache-beam-temp-bucket-123e4567-e89b-42d3-a456-426614174001"


class FakeBlob:
    def __init__(self, bucket, name, generation):
        self.bucket = bucket
        self.name = name
        self.generation = generation

    def delete(self, if_generation_match=None):
        if if_generation_match != self.generation:
            raise ValueError("blob generation precondition is required")
        self.bucket.blobs.remove(self)


class FakeBucket:
    def __init__(self, name, created, blob_count=0):
        self.name = name
        self.time_created = created
        self.blobs = [FakeBlob(self, f"blob-{i}", i + 1)
                      for i in range(blob_count)]
        self.deleted = False
        self.delete_error = None

    def list_blobs(self, versions=False):
        if not versions:
            raise ValueError("all object generations must be listed")
        return iter(list(self.blobs))

    def delete(self):
        if self.delete_error:
            raise self.delete_error
        if self.blobs:
            raise ValueError("bucket is not empty")
        self.deleted = True


class FakeClient:
    def __init__(self):
        self.buckets = []

    def list_buckets(self, project, prefix):
        if project != "apache-beam-testing" or prefix != "apache-beam-temp-bucket-":
            raise ValueError("unexpected bucket selection")
        return iter(self.buckets)


def bucket(name, created, blob_count=0):
    return FakeBucket(name, created, blob_count)


class StaleGcsBucketsCleanerTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()

    def test_deletes_only_stale_test_buckets(self):
        stale = bucket(
            STALE_BUCKET, NOW - datetime.timedelta(hours=25))
        fresh = bucket(
            FRESH_BUCKET, NOW - datetime.timedelta(hours=23))
        unrelated = bucket("other-bucket", NOW - datetime.timedelta(days=7))
        missing_created = bucket(
            "apache-beam-temp-bucket-123e4567-e89b-42d3-a456-426614174002",
            None)
        self.client.buckets = [
            stale, fresh, unrelated, missing_created]

        deleted = cleaner.clean_stale_gcs_buckets(
            self.client, now=NOW, dry_run=False)

        self.assertEqual(deleted, [stale.name])
        self.assertTrue(stale.deleted)
        self.assertFalse(fresh.deleted)
        self.assertFalse(unrelated.deleted)
        self.assertFalse(missing_created.deleted)

    def test_preserves_bucket_at_age_threshold(self):
        value = bucket(
            STALE_BUCKET,
            NOW - datetime.timedelta(hours=24))
        self.client.buckets = [value]

        deleted = cleaner.clean_stale_gcs_buckets(
            self.client, now=NOW, dry_run=False)

        self.assertEqual(deleted, [])
        self.assertFalse(value.deleted)

    def test_dry_run_does_not_delete_bucket(self):
        value = bucket(
            STALE_BUCKET, NOW - datetime.timedelta(days=2))
        self.client.buckets = [value]

        selected = cleaner.clean_stale_gcs_buckets(self.client, now=NOW)

        self.assertEqual(selected, [value.name])
        self.assertFalse(value.deleted)

    def test_naive_creation_time_is_treated_as_utc(self):
        value = bucket(
            STALE_BUCKET,
            datetime.datetime(2026, 9, 29, 11))
        self.client.buckets = [value]

        deleted = cleaner.clean_stale_gcs_buckets(
            self.client, now=NOW, dry_run=False)

        self.assertEqual(deleted, [value.name])
        self.assertTrue(value.deleted)

    def test_continues_after_delete_failure_and_reports_all_failures(self):
        failed = bucket(
            STALE_BUCKET, NOW - datetime.timedelta(days=2))
        deleted = bucket(
            FRESH_BUCKET, NOW - datetime.timedelta(days=2))
        failed.delete_error = RuntimeError("permission denied")
        self.client.buckets = [failed, deleted]

        with self.assertRaisesRegex(
            RuntimeError, "Bucket failure count: 1"):
            cleaner.clean_stale_gcs_buckets(
                self.client, now=NOW, dry_run=False)

        self.assertFalse(failed.deleted)
        self.assertTrue(deleted.deleted)

    def test_deletes_bucket_with_more_than_256_objects(self):
        value = bucket(STALE_BUCKET, NOW - datetime.timedelta(days=2), 300)
        self.client.buckets = [value]

        selected = cleaner.clean_stale_gcs_buckets(
            self.client, now=NOW, dry_run=False)

        self.assertEqual(selected, [value.name])
        self.assertTrue(value.deleted)
        self.assertEqual(value.blobs, [])

    def test_preserves_prefixed_bucket_without_uuid_name(self):
        value = bucket(
            "apache-beam-temp-bucket-manual", NOW - datetime.timedelta(days=7))
        self.client.buckets = [value]

        deleted = cleaner.clean_stale_gcs_buckets(self.client, now=NOW)

        self.assertEqual(deleted, [])
        self.assertFalse(value.deleted)

    @mock.patch("stale_gcs_buckets_cleaner.clean_stale_gcs_buckets")
    @mock.patch("stale_gcs_buckets_cleaner.storage.Client")
    def test_main_defaults_to_dry_run(self, client_class, clean):
        clean.return_value = []

        with mock.patch("sys.argv", ["stale_gcs_buckets_cleaner.py"]):
            cleaner.main()

        client_class.assert_called_once_with(project="apache-beam-testing")
        clean.assert_called_once_with(client_class.return_value, dry_run=True)

    @mock.patch("stale_gcs_buckets_cleaner.clean_stale_gcs_buckets")
    @mock.patch("stale_gcs_buckets_cleaner.storage.Client")
    def test_main_requires_delete_flag_for_deletion(self, client_class, clean):
        clean.return_value = []

        with mock.patch(
            "sys.argv", ["stale_gcs_buckets_cleaner.py", "--delete"]):
            cleaner.main()

        clean.assert_called_once_with(client_class.return_value, dry_run=False)


if __name__ == "__main__":
    unittest.main()
