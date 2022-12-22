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
# Logical types

There may be cases when you need to extend the schema type system to add custom logical types. A unique identifier and an argument identify a logical type. Apart from defining the underlying schema type for storage, you also need to implement to and from type conversions. For example, you can represent the union logical type as a row with nullable fields, with only one field set at a time.