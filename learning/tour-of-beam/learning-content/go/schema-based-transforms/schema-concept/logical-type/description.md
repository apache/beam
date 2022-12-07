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

Users can extend the schema type system to add custom logical types that can be used as a field. A logical type is identified by a unique identifier and an argument. A logical type also specifies an underlying schema type to be used for storage, along with conversions to and from that type. As an example, a logical union can always be represented as a row with nullable fields, where the user ensures that only one of those fields is ever set at a time.