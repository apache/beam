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

# PR Bot Commands

The following commands are available for interaction with the PR Bot
All commands are case insensitive.

| Command      | Description |
| ----------- | ----------- |
| `r: @username` | Ask someone for a review. This will disable the bot for the PR since it assumes you are able to find a reviewer. |
| `assign to next reviewer` | If someone has been assigned to a PR by the bot, this unassigns them and picks a new reviewer. Useful if you don't have the bandwitdth or context to review. |
| `stop reviewer notifications` | This will disable the bot for the PR. |
| `remind me after tests pass` | This will comment after all checks complete and tag the person who commented the command. |
| `waiting on author` | This shifts the attention set to the author. The author can shift the attention set back to the reviewer by commenting anywhere or pushing. |
| `assign set of reviewers` | If the bot has not yet assigned a set of reviewers to the PR, this command will trigger that happening. |