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


def bucket(name, created):
    value = mock.Mock()
    value.name = name
    value.time_created = created
    return value


class StaleGcsBucketsCleanerTest(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()

    def test_deletes_only_stale_test_buckets(self):
        stale = bucket(
            STALE_BUCKET, NOW - datetime.timedelta(hours=25))
        fresh = bucket(
            FRESH_BUCKET, NOW - datetime.timedelta(hours=23))
        unrelated = bucket("other-bucket", NOW - datetime.timedelta(days=7))
        missing_created = bucket(
            "apache-beam-temp-bucket-123e4567-e89b-42d3-a456-426614174002",
            None)
        self.client.list_buckets.return_value = [
            stale, fresh, unrelated, missing_created]

        deleted = cleaner.clean_stale_gcs_buckets(
            self.client, now=NOW, dry_run=False)

        self.assertEqual(deleted, [stale.name])
        stale.delete.assert_called_once_with(force=True)
        fresh.delete.assert_not_called()
        unrelated.delete.assert_not_called()
        missing_created.delete.assert_not_called()
        self.client.list_buckets.assert_called_once_with(
            project="apache-beam-testing", prefix="apache-beam-temp-bucket-")

    def test_preserves_bucket_at_age_threshold(self):
        value = bucket(
            STALE_BUCKET,
            NOW - datetime.timedelta(hours=24))
        self.client.list_buckets.return_value = [value]

        deleted = cleaner.clean_stale_gcs_buckets(
            self.client, now=NOW, dry_run=False)

        self.assertEqual(deleted, [])
        value.delete.assert_not_called()

    def test_dry_run_does_not_delete_bucket(self):
        value = bucket(
            STALE_BUCKET, NOW - datetime.timedelta(days=2))
        self.client.list_buckets.return_value = [value]

        selected = cleaner.clean_stale_gcs_buckets(self.client, now=NOW)

        self.assertEqual(selected, [value.name])
        value.delete.assert_not_called()

    def test_naive_creation_time_is_treated_as_utc(self):
        value = bucket(
            STALE_BUCKET,
            datetime.datetime(2026, 9, 29, 11))
        self.client.list_buckets.return_value = [value]

        deleted = cleaner.clean_stale_gcs_buckets(
            self.client, now=NOW, dry_run=False)

        self.assertEqual(deleted, [value.name])
        value.delete.assert_called_once_with(force=True)

    def test_continues_after_delete_failure_and_reports_all_failures(self):
        failed = bucket(
            STALE_BUCKET, NOW - datetime.timedelta(days=2))
        deleted = bucket(
            FRESH_BUCKET, NOW - datetime.timedelta(days=2))
        failed.delete.side_effect = RuntimeError("permission denied")
        self.client.list_buckets.return_value = [failed, deleted]

        with self.assertRaisesRegex(
            RuntimeError, "Failed to delete 1 stale GCS bucket"):
            cleaner.clean_stale_gcs_buckets(
                self.client, now=NOW, dry_run=False)

        failed.delete.assert_called_once_with(force=True)
        deleted.delete.assert_called_once_with(force=True)

    def test_preserves_prefixed_bucket_without_uuid_name(self):
        value = bucket(
            "apache-beam-temp-bucket-manual", NOW - datetime.timedelta(days=7))
        self.client.list_buckets.return_value = [value]

        deleted = cleaner.clean_stale_gcs_buckets(self.client, now=NOW)

        self.assertEqual(deleted, [])
        value.delete.assert_not_called()

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
