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

# PR Bot

This directory holds all the code (except for Actions Workflows) for our PR bot designed to improve the PR experience.
For a list of commands to use when interacting with the bot, see [Commands.md](./Commands.md).
For a design doc explaining the design and implementation, see [Automate Reviewer Assignment](https://docs.google.com/document/d/1FhRPRD6VXkYlLAPhNfZB7y2Yese2FCWBzjx67d3TjBo/edit#)

## Build/Test

To build, run:

```
npm install
npm run build
```

To run the tests:

```
npm test
```

Before checking in code, run prettier on it:

```
npm run format
```