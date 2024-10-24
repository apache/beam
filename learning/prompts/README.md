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
This folder holds golden prompt/response pairs for Google Duet AI training.

A golden prompt/response pair contains two parts:
1. An example prompt/question to ask an LLM
2. An ideal answer we would expect the LLM to generate

Each prompt/response pair is a markdown file with the following structure:
```
Prompt:
<markdown for prompt>

Response:
<markdown for response>
```
This folder includes the following types of golden prompts:
- Documentation lookup prompts
- Code generation prompts
- Code explanation prompts

## Documentation lookup prompts
Features of a good response:
- Uses official product names in the response (“Speech to text” → “Speech-to-Text”).
- Answers the question (correctly) with hyperlinks to the documentation.
- Includes a link to the corresponding source code.
- Includes a link to samples, if available.


## Code generation prompts
Features of a good response:
- Starts with a brief introduction that explains the code sample.
- Includes information about how to find the reference documentation.
- Includes a link to the list of code samples.
- Provides well-documented code. Consider including an example of what the execution result looks like.
- Follows up with the user to ensure they don’t continue needlessly with false responses.


## Code explanation prompts
Features of a good response:
- Starts with a short overall description that tries to answer the question in the prompt.
- Grounds the algorithm in any well-known context, if appropriate. For example, this is an implementation of X, a well-known algorithm to do Y.
- Discusses the variables in the snippet and their purpose relative to the runtime.
- Discusses runtime and memory storage complexity.
- Notes any interesting features of the code, or opportunities for improvement (optimizations, refactoring, syntax best practices, etc.)

Folder structure:
```
learning/prompts/
├── code-explanation
│   ├── 01_io_kafka.md
│   └── ...
├── code-generation
│   ├── 01_io_kafka.md
│   └── ...
├── documentation-lookup
│   ├── 01_basic_learning_apache_beam.md
│   └── ...
└── README.md
```
