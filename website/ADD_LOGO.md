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

# How to add your logo
Please follow these steps to add your company or project logo to Apache Beam [case studies](https://beam.apache.org/case-studies/) page:
1. Fork [Apache Beam](https://github.com/apache/beam) repository
2. Add file with company or project name to
   the [case-studies](https://github.com/apache/beam/tree/master/website/www/site/content/en/case-studies) folder
   e.g., `company.md`
3. Add company/project logo to
   the [images/logos/powered-by](https://github.com/apache/beam/tree/master/website/www/site/static/images/logos/powered-by)
   folder. Please use your company/project name e.g. `ricardo.png`
4. Copy template below to the created file and replace next fields with your data

| Field             | Description                                                                                             |
|-------------------|---------------------------------------------------------------------------------------------------------|
| `title`           | Company/project name                                                                                    |
| `icon`            | Path to the company/project logo e.g. "/images/logos/powered-by/company_name.png"                       |
| `hasNav`          | Specified logo page has space for left & right nav menu                                                 |
| `hasLink`         | Links logo image to the company/project website instead of displaying cardDescription, optional         |
| `cardDescription` | Company or project description, optional                                                                |

```
---
title: "Cloud Dataflow"
icon: /images/logos/powered-by/company_name.png
hasNav: true
hasLink: false
cardDescription: "Google Cloud Dataflow is a fully managed service for executing Apache Beam pipelines within the Google Cloud Platform ecosystem."
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
```

5. Create pull request to the Apache Beam repository with your changes