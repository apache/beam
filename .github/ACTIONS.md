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

> **PLEASE update this file if you add new github action or change name/trigger phrase of a github action.**

## Beam Github Actions

### Issue Management

Phrases self-assign, close, or manage labels on an issue:
| Phrase | Effect |
|--------|--------|
| `.take-issue` | Self-assign the issue |
| `.close-issue` | Close the issue as completed |
| `.close-issue not_planned` | Close the issue as not-planned |
| `.add-labels` | Add comma separated labels to the issue (e.g. `add-labels l1, 'l2 with spaces'`) |
| `.remove-labels` | Remove comma separated labels to the issue (e.g. `remove-labels l1, 'l2 with spaces'`) |
| `.set-labels` | Sets comma separated labels to the issue and removes any other labels (e.g. `set-labels l1, 'l2 with spaces'`) |
