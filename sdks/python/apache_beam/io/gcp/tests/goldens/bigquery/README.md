<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# BigQueryWrapper wire-level golden files

These files record the HTTP requests that `BigQueryWrapper`
(`apache_beam/io/gcp/bigquery_tools.py`) sends to the BigQuery REST API. They
are the regression baseline for moving the wrapper off apitools and onto
`google-cloud-bigquery`. After that move, a golden case must still pass
unchanged, unless a reviewer approves a documented difference.

Tests that use them:

* `apache_beam/io/gcp/bigquery_wrapper_golden_test.py` covers cases G1–G40.
* `apache_beam/io/gcp/tests/bigquery_http_recorder.py` is the harness: it
  records requests, serves canned responses, normalizes requests, and
  compares or writes golden files.

Each file was captured from the transport layer (`requests`/`httplib2`), which
sits below both client libraries. The goldens therefore describe the wire
contract and are independent of which library sends the request.

JSON can't carry comments, so these files have no license header. The root
`build.gradle.kts` excludes `**/*.json` from Apache RAT.

## File format

```json
{
  "case": "get_table__basic",
  "description": "get_table('p','d','t')",
  "captured_from": "master@5d9ab78838a (apitools control plane, google-cloud-bigquery insertAll)",
  "requests": [
    {
      "method": "GET",
      "path": "/bigquery/v2/projects/p/datasets/d/tables/t",
      "query": {},
      "body": null
    }
  ]
}
```

* `case` is the file name without `.json` and is the ID passed to
  `assert_matches_golden`.
* `description` is a human-readable summary of the call under test.
* `captured_from` records the commit and libraries that produced the file.
  It is metadata only and is never compared.
* `requests` lists every request in the order it was sent, in the
  normalized form described below. Retries appear as repeated entries.

Files are written with `json.dumps(indent=2, sort_keys=True,
ensure_ascii=False)` and a trailing newline, so diffs stay stable.

## Normalization rules

A request is normalized before it is compared. The rules are implemented in
`bigquery_http_recorder.normalize`.

| Rule | What it does |
|---|---|
| N1 | Keeps the path from `/bigquery/v2/` or `/upload/bigquery/v2/` onward and drops the scheme and host. Percent-encoding is kept exactly as sent. For example, `google.com:p` is sent unencoded by both libraries today. |
| N2 | Parses the query string into a sorted dict and drops the transport-only params `alt`, `prettyPrint`, `fields` and `$.xgafv`. |
| N3 | Parses JSON bodies and sorts keys recursively. |
| N4 | Removes keys whose value is `null`, at every depth. This also applies to `insertAll` rows, so a column set to `null` and a missing column look the same. The service treats them the same for `insertAll`. |
| N5 | Replaces `beam_temp_(dataset\|table)_<32 hex>` with `beam_temp_\1_<UUID>`. For cases that pass `generated_job_id=True`, every standalone 32-hex token (such as generated job IDs and `insertId`s) becomes `<UUID>`. |
| N6 | Splits `multipart/related` upload bodies into `{"metadata": <json>, "media": {"content_type": ..., "length": ..., "sha256": ...}}`. The random boundary is dropped, and the media payload is recorded only by length and SHA-256. |
| N7 | Excludes headers entirely: auth, user-agent, content-length and so on. |

> **Not implemented: proposed N8 (boolean query casing).** apitools sends
> boolean query parameters in Python casing, for example
> `deleteContents=True` (see `delete_dataset__contents.json`).
> `google-cloud-bigquery` sends `true`. The goldens keep the raw casing on
> purpose. The migration phase should either add N8 (case-insensitive
> comparison of boolean query values) or regenerate the affected goldens
> with reviewer sign-off.

## Updating goldens

Goldens change only through update mode. Never edit them by hand, and never
add per-Python-version or per-library variants.

```sh
cd sdks/python
BEAM_UPDATE_BQ_GOLDENS=1 \
BEAM_BQ_GOLDENS_CAPTURED_FROM="<branch>@<sha> (<libraries>)" \
  pytest apache_beam/io/gcp/bigquery_wrapper_golden_test.py
git diff apache_beam/io/gcp/tests/goldens/bigquery/
```

* In update mode, a file is rewritten only when its `requests`,
  `description` or `case` changed. Running update mode on an unchanged tree
  therefore changes nothing.
* `BEAM_BQ_GOLDENS_CAPTURED_FROM` is optional. If it is unset, a rewritten
  file keeps its existing `captured_from` value.
* Outside update mode, a missing golden file fails the test. It is never
  created silently.
* **Reviewer sign-off is required** for any diff under this directory. The
  PR description must explain why each changed request is still correct on
  the wire.

## Coverage checklist

Every network-touching `BigQueryWrapper` method and the golden cases that
cover it:

| Method | Cases |
|---|---|
| `get_table` | G1 `get_table__basic`, G2 `get_table__domain_scoped_project` |
| `_create_table` | G3 `create_table__schema_only`, G4 `create_table__additional_params` (G4b: invalid params are rejected before any request) |
| `get_or_create_table` | G5 `…__exists_append`, G6 `…__missing_create`, G7 `…__truncate`, G8 `…__write_empty_nonempty`, G9 `…__create_conflict` |
| `_is_table_empty` | G10 `is_table_empty` (also inside G7 and G8) |
| `_delete_table` | G11 `delete_table__ok`, `delete_table__404` (also inside G7) |
| `get_or_create_dataset` | G12 `get_or_create_dataset__exists`, G13 `…__create_full` |
| `create_temporary_dataset` | G14 `create_temporary_dataset__beam_generated`, G15 `…__preexisting_raises` |
| `_delete_dataset` | G16 `delete_dataset__contents`, `delete_dataset__404` |
| `clean_up_temporary_dataset` | G17 `…__generated`, G18 `…__user_configured`, G19 `…__403` |
| `_clean_up_beam_labelled_temporary_datasets` | G20 `clean_up_labelled_datasets` |
| `_insert_load_job` / `perform_load_job` | G21 `insert_load_job__uris_schema_labels`, G22 `…__autodetect`, G23 `…__additional_params`, G24 `…__source_stream` (one file, two requests), G25 `…__load_job_project_id` |
| `_insert_copy_job` | G26 `insert_copy_job__basic` |
| `_start_job` (including `_parse_location_from_exc`) | G27 `start_job__409_location_parse` (also inside G21–G30) |
| `perform_extract_job` | G28 `perform_extract_job__avro`, `…__json_gzip` |
| `_start_query_job` | G29 `start_query_job__standard`, G30 `…__dry_run` |
| `get_query_location` | G31 `get_query_location__referenced_tables` |
| `get_table_location` | G31 (called by `get_query_location` for each referenced table) |
| `get_job` | G32 `get_job__with_location` |
| `wait_for_bq_job` | G33 `wait_for_bq_job__running_then_done`, G34 `…__done_with_error` |
| `_get_query_results` / `run_query` | G35 `run_query__paginated`, G36 `run_query__row_shape` |
| `_insert_all_rows` / `insert_rows` | G37 `insert_rows__basic`, G38 `…__flags_and_encoding`, G39 `…__domain_scoped_project`, G40 `…__row_errors` |

Retry, error-mapping and metrics behavior is not recorded here. It is covered
by `apache_beam/io/gcp/bigquery_wrapper_characterization_test.py`.
