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

# Overview

Most structured records share some common characteristics:

→  They can be subdivided into separate named fields. Fields usually have string names, but sometimes - as in the case of indexed tuples - have numerical indices instead.

→  There is a confined list of primitive types that a field can have. These often match primitive types in most programming languages: int, long, string, etc.

→  Often a field type can be marked as optional (sometimes referred to as nullable) or required.

Often records have a nested structure. A nested structure occurs when a field itself has subfields so the type of the field itself has a schema. Fields that are array or map types is also a common feature of these structured records.

For example, consider the following schema, representing actions in a fictitious e-commerce company:

**Transaction**

```
Field Name              Field Type
bank                    STRING
purchaseAmount          DOUBLE
```