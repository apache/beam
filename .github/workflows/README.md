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
Please note that jobs with matrix need to have matrix element in the comment. Example:
```Run Python PreCommit (3.8)```
| Workflow name | Matrix | Trigger Phrase | Cron Status |
|:-------------:|:------:|:--------------:|:-----------:|
| [ Go PreCommit ](https://github.com/apache/beam/actions/workflows/beam_PreCommit_Go.yml) | N/A |`Run Go PreCommit`| [![Go PreCommit](https://github.com/apache/beam/actions/workflows/beam_PreCommit_Go.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/beam_PreCommit_Go.yml) |
| [ Python PreCommit Docker ](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_DockerBuild.yml) | ['3.8','3.9','3.10','3.11'] | `Run PythonDocker PreCommit (matrix_element)`| [![.github/workflows/job_PreCommit_Python_DockerBuild.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_DockerBuild.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_DockerBuild.yml) |
| [ Python PreCommit Docs ](https://github.com/apache/beam/actions/workflows/beam_PreCommit_PythonDocs.yml) | N/A | `Run PythonDocs PreCommit`| [![.github/workflows/beam_PreCommit_PythonDocs.yml](https://github.com/apache/beam/actions/workflows/beam_PreCommit_PythonDocs.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/beam_PreCommit_PythonDocs.yml) |
| [ Python PreCommit Formatter ](https://github.com/apache/beam/actions/workflows/job_PreCommit_PythonAutoformatter.yml) | N/A | `Run PythonFormatter PreCommit`| [![.github/workflows/job_PreCommit_PythonAutoformatter.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_PythonAutoformatter.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_PythonAutoformatter.yml) |
| [ Python PreCommit Coverage ](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Coverage.yml) | N/A | `Run Python_Coverage PreCommit`| [![.github/workflows/job_PreCommit_Python_Coverage.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Coverage.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Coverage.yml) |
| [ Python PreCommit Dataframes ](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Dataframes.yml) | ['3.8','3.9','3.10','3.11'] | `Run Python_Dataframes PreCommit (matrix_element)`| [![.github/workflows/job_PreCommit_Python_Dataframes.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Dataframes.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Dataframes.yml) |
| [ Python PreCommit Examples ](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Examples.yml) | ['3.8','3.9','3.10','3.11'] | `Run Python_Examples PreCommit (matrix_element)` | [![.github/workflows/job_PreCommit_Python_Examples.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Examples.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Examples.yml) |
| [ Python PreCommit Runners ](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Runners.yml) | ['3.8','3.9','3.10','3.11'] | `Run Python_Runners PreCommit (matrix_element)`| [![.github/workflows/job_PreCommit_Python_Runners.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Runners.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Runners.yml) |
| [ Python PreCommit Lint ](https://github.com/apache/beam/actions/workflows/job_PreCommit_PythonLint.yml) | N/A | `Run PythonLint PreCommit` | [![.github/workflows/job_PreCommit_PythonLint.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_PythonLint.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_PythonLint.yml) |
| [ Python PreCommit Transforms ](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Transforms.yml) | ['3.8','3.9','3.10','3.11'] | `Run Python_Transforms PreCommit (matrix_element)`| [![.github/workflows/job_PreCommit_Python_Transforms.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Transforms.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python_Transforms.yml) |
| [ Python PreCommit ](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python.yml) | ['3.8','3.9','3.10','3.11'] | `Run Python PreCommit (matrix_element)` | [![.github/workflows/job_PreCommit_Python.yml](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/job_PreCommit_Python.yml) |
| [ PreCommit Python Integration](https://github.com/apache/beam/actions/workflows/beam_PreCommit_Python_Integration.yml) | ['3.8','3.11'] | `Run Python_Integration PreCommit (matrix_element)` | [![.github/workflows/beam_PreCommit_Python_Integration.yml](https://github.com/apache/beam/actions/workflows/beam_PreCommit_Python_Integration.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/beam_PreCommit_Python_Integration.yml) |
| [ RAT PreCommit ](https://github.com/apache/beam/actions/workflows/beam_PreCommit_RAT.yml) | N/A | `Run RAT PreCommit` | [![.github/workflows/beam_PreCommit_RAT.yml](https://github.com/apache/beam/actions/workflows/beam_PreCommit_RAT.yml/badge.svg?event=schedule)](https://github.com/apache/beam/actions/workflows/beam_PreCommit_RAT.yml) |
