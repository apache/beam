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

1. Fork [Apache Beam](https://github.com/apache/beam) repository
2. Add file with company or project name to
   the [case-studies](https://github.com/apache/beam/tree/master/website/www/site/content/en/case-studies) folder
   e.g., `company.md`
3. Add project/company logo to
   the [images/logos/powered-by](https://github.com/apache/beam/tree/master/website/www/site/static/images/logos/powered-by)
   folder. Please use your company/project name e.g. `ricardo.png`
4. Copy template below to the created file and replace next fields with your data

| Field           | Name                                             |
|-----------------|--------------------------------------------------|
| title           | Project/Company name                             |
| icon            | Path to the logo e.g. "/images/company_name.png" |
| cardDescription | Description of the project                       |

```
---
title: "Cloud Dataflow"
icon: /images/company_name.png
cardDescription: "Project/Company description"
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

5. Create pull request to the apache beam repository with your changes
