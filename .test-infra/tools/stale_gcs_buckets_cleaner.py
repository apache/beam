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
"""Deletes stale GCS buckets left behind by GcsUtil integration tests."""

import argparse
import datetime
import re

from google.cloud import storage


DEFAULT_PROJECT_ID = "apache-beam-testing"
GCS_TEMP_BUCKET_PREFIX = "apache-beam-temp-bucket-"
GCS_TEMP_BUCKET_PATTERN = re.compile(
    rf"^{GCS_TEMP_BUCKET_PREFIX}"
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-"
    r"[0-9a-f]{12}$")
DEFAULT_MAX_AGE = datetime.timedelta(hours=24)
UTC = datetime.timezone.utc


def _as_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def clean_stale_gcs_buckets(
    client,
    max_age=DEFAULT_MAX_AGE,
    now=None,
    dry_run=True,
):
    """Deletes test buckets whose creation time is older than ``max_age``.

    Returns the names of buckets selected for deletion. If a deletion fails,
    the cleaner attempts the remaining buckets before reporting the failures.
    """
    current_time = _as_utc(now or datetime.datetime.now(UTC))
    selected = []
    failures = []

    buckets = client.list_buckets(
        project=DEFAULT_PROJECT_ID, prefix=GCS_TEMP_BUCKET_PREFIX)
    for bucket in buckets:
        # Keep this check even though list_buckets filters server-side. It is
        # the final guard before a destructive operation.
        if not GCS_TEMP_BUCKET_PATTERN.fullmatch(bucket.name):
            continue
        if bucket.time_created is None:
            print(f"Skipping {bucket.name}: creation time is unavailable")
            continue
        if current_time - _as_utc(bucket.time_created) <= max_age:
            continue

        selected.append(bucket.name)
        if dry_run:
            print(f"Dry run: would delete gs://{bucket.name}")
            continue

        print(f"Deleting stale test bucket gs://{bucket.name}")
        try:
            # Tests can leave objects behind when they fail before teardown.
            bucket.delete(force=True)
        except Exception as error:
            failures.append(bucket.name)
            print(f"Failed to delete gs://{bucket.name}: {error}")

    if failures:
        noun = "bucket" if len(failures) == 1 else "buckets"
        raise RuntimeError(
            f"Failed to delete {len(failures)} stale GCS {noun}: "
            f"{', '.join(failures)}")

    return selected


def main():
    parser = argparse.ArgumentParser(
        description="Delete stale buckets created by GcsUtil integration tests.")
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete eligible buckets instead of performing a dry run.",
    )
    args = parser.parse_args()

    client = storage.Client(project=DEFAULT_PROJECT_ID)
    selected = clean_stale_gcs_buckets(client, dry_run=not args.delete)
    action = "Deleted" if args.delete else "Selected"
    print(f"{action} {len(selected)} stale GCS test bucket(s)")


if __name__ == "__main__":
    main()
