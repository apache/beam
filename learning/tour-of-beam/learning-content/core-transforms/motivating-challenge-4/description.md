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
### Core Transforms motivating challenge-4

You need to group the words so that the key is the first letter and the value is an array of words. This can be achieved with `CombineFn`.
{{if (eq .Sdk "go")}}
> **Importantly**. Since Go `DirectRunner` cannot parallelize, `MergeAccumulators` cannot be executed. Therefore, its logic will be contained in `AddInput`.

{{end}}