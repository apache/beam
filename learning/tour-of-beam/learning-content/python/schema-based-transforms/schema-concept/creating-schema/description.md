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

* They can be subdivided into separate named fields. Fields usually have string names, but sometimes - as in the case of indexed tuples - have numerical indices instead.
* There is a confined list of primitive types that a field can have. These often match primitive types in most programming languages: int, long, string, etc.
* Often a field type can be marked as optional (sometimes referred to as nullable) or required.

Often records have a nested structure. A nested structure occurs when a field itself has subfields so the type of the field itself has a schema. Fields that are array or map types is also a common feature of these structured records.

For example, consider the following schema, representing actions in a fictitious e-commerce company:

**Transaction**

```
Field Name              Field Type
bank                    STRING
purchaseAmount          DOUBLE
```

### Creating Schemas
While schemas themselves are language independent, they are designed to embed naturally into the programming languages of the Beam SDK being used. This allows Beam users to continue using native types while reaping the advantage of having Beam understand their element schemas.

Beam has a few different mechanisms for inferring schemas from Python code.

### NamedTuple classes
A `NamedTuple` class is a Python class that wraps a tuple, assigning a name to each element and restricting it to a particular type. Beam will automatically infer the schema for PCollections with NamedTuple output types. For example:

```
class Transaction(typing.NamedTuple):
  bank: str
  purchase_amount: float

pc = input | beam.Map(lambda ...).with_output_types(Transaction)
```

### beam.Row and Select

There are also methods for creating ad-hoc schema declarations. First, you can use a lambda that returns instances of `beam.Row`:
```
input_pc = ... # {"bank": ..., "purchase_amount": ...}
output_pc = input_pc | beam.Map(lambda item: beam.Row(bank=item["bank"],
                                                      purchase_amount=item["purchase_amount"]
```

Sometimes it can be more concise to express the same logic with the `Select` transform:

```
input_pc = ... # {"bank": ..., "purchase_amount": ...}
output_pc = input_pc | beam.Select(bank=lambda item: item["bank"],
                                   purchase_amount=lambda item: item["purchase_amount"])
```

Note that these declaration don’t include any specific information about the types of the `bank` and `purchase_amount` fields, so Beam will attempt to infer type information. If it’s unable to it will fall back to the generic type `Any`. Sometimes this is not ideal, you can use casts to make sure Beam correctly infers types with `beam.Row` or with `Select`:

```
input_pc = ... # {"bank": ..., "purchase_amount": ...}
output_pc = input_pc | beam.Map(lambda item: beam.Row(bank=str(item["bank"]),
                                                      purchase_amount=float(item["purchase_amount"])))
```