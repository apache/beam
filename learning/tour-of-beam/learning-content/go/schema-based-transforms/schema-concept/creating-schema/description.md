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

&#8594; They can be subdivided into separate named fields. Fields usually have string names, but sometimes - as in the case of indexed tuples - have numerical indices instead.

&#8594; There is a confined list of primitive types that a field can have. These often match primitive types in most programming languages: int, long, string, etc.

&#8594; Often a field type can be marked as optional (sometimes referred to as nullable) or required.

Often records have a nested structure. A nested structure occurs when a field itself has subfields so the type of the field itself has a schema. Fields that are array or map types is also a common feature of these structured records.

For example, consider the following schema :

**Transaction**

```
Field Name	        Field Type
bank	                STRING
purchase_amount	        DOUBLE
```

### Creating Schemas

While schemas themselves are language independent, they are designed to embed naturally into the programming languages of the Beam SDK being used. This allows Beam users to continue using native types while reaping the advantage of having Beam understand their element schemas.

Beam currently only infers schemas for exported fields in Go structs.

#### Structs

Beam will automatically infer schemas for all Go structs used as `PCollection` elements, and default to encoding them using schema encoding.

```
type Transaction struct{
  Bank string
  PurchaseAmount float64

  checksum []byte // ignored
}
```

Unexported fields are ignored, and cannot be automatically infered as part of the schema. Fields of type func, channel, unsafe.Pointer, or uintptr will be ignored by inference. Fields of interface types are ignored, unless a schema provider is registered for them.

By default, schema field names will match the exported struct field names. In the above example, “Bank” and “PurchaseAmount” are the schema field names. A schema field name can be overridden with a struct tag for the field.

```
type Transaction struct{
  Bank           string  `beam:"bank"`
  PurchaseAmount float64 `beam:"purchase_amount"`
}
```

Overriding schema field names is useful for compatibility cross language transforms, as schema fields may have different requirements or restrictions from Go exported fields.

