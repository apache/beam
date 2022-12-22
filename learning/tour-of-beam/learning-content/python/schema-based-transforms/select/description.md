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

# Select

The `Select` transform allows one to easily project out only the fields of interest. The resulting `PCollection` has a schema containing each selected field as a top-level field. Both top-level and nested fields can be selected.

The output of this transform is of type Row, though that can be converted into any other type with matching schema using the `Convert` transform.

Sometimes different nested rows will have fields with the same name. Selecting multiple of these fields would result in a name conflict, as all selected fields are put in the same row schema. When this situation arises, the Select.withFieldNameAs builder method can be used to provide an alternate name for the selected field.