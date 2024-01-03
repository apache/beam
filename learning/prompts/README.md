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
There are three types of golden prompts in this folder:
- documentaion lookup prompts
- code generation prompts
- code explanation prompts

## Documentation lookup prompts
Features of a good response:
- uses official product names in the response (“Speech to text” → “Speech-to-Text”)
- answers the question (correctly) with hyperlinks to the documentation
- includes a link to the corresponding source code.
- includes a link to samples, if available


## Code generation prompts
Features of a good response:
- prelude explaining the code sample
- include information about how to find the reference documentation
- include a link to the list of code samples
- well documented code. Consider returning an example of what the return result would look like.
- follow up to the user to ensure they don’t perseverate on false responses.


## Code explanation prompts
Features of a good response:
- starts with a short overall description of tries to answer the question in the prompt.
- grounds the algorithm in any well-known context (this is an implementation of X, a well-known algorithm to do Y) if appropriate.
- discusses the variables in the snippet, and what their purpose is relative to the runtime.
- discusses runtime and memory storage complexity.
- make note of any interesting features of the code, or opportunities for improvement (optimizations, refactor, syntax best-practices, etc.)

Folder structure:
```
learning/prompts/
├── code-explaination
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