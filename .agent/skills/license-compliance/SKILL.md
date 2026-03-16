---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name: license-compliance
description: Adds, validates, and formats Apache 2.0 license headers for all file types in Apache Beam. Use when creating new files, fixing RAT check failures, adding copyright headers, or checking license compliance.
---

# License Compliance in Apache Beam

## Workflow
1. Create new file
2. Add the appropriate license header from templates below (must be the very first content)
3. Run `./gradlew rat` to validate
4. If failures, check `build/reports/rat/index.html` for details
5. Fix any missing headers and re-run until passing

## License Headers by File Type

### Java, Groovy, Kotlin, Scala
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

### Python
```python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
```

### Go (`//` comments)
Same license text as above using `//` comment prefix.

### Python, YAML, Shell (`#` comments)
Same license text using `#` comment prefix. For shell scripts, place after `#!/bin/bash` shebang.

### Markdown, XML, HTML (`<!-- -->` comments)
```xml
<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. [full Apache 2.0 text]
-->
```

## RAT Check

### Running Locally
```bash
./gradlew rat
```

### Checking Results
If the RAT check fails, view the report:
```
build/reports/rat/index.html
```

## Key Rules

1. **Every new file needs a license header** - No exceptions for source files
2. **Place header at the very top** - Before any code, imports, or declarations
3. **Use correct comment style** - Match the file type's comment syntax
4. **YAML frontmatter exception** - For files with YAML frontmatter (like SKILL.md), place the license as YAML comments inside the frontmatter block, after the opening `---`
5. **Dependencies must be Apache-compatible** - New dependencies need licenses compatible with Apache 2.0

## Common Mistakes

- Forgetting to add headers to new test files
- Missing headers on configuration files (.yaml, .json, .xml)
- Adding HTML comments before YAML frontmatter (breaks parsing)
