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

The `Select` transform allows one to easily project out only the fields of interest. The resulting `PCollection` has a schema containing each selected field as a top-level field. You can choose both top-level and nested fields.

The output of this transform is of type Row, which you can convert into any other type with matching schema using the `Convert` transform.

### Top-level fields

To select a field at the top level of a schema, you need to specify their names. For example, using the following code, you can choose just user ids from a `PCollection` of purchases:

```
PCollection<Row> rows = input.apply(Select.fieldNames("userId", "shippingAddress.postCode"));
```

Will result in the following schema:

```
Field Name       Field Type
userId           STRING
```

### Nested fields

Individual nested fields can be specified using the dot operator. For example, you can select just the postal code from the shipping address using the following:

```
PCollection<Row> rows = input.apply(Select.fieldNames("shippingAddress.userId","shippingAddress.postCode","shippingAddress.email"));
```

Will result in the following schema:

```
Field Name       Field Type
userId           INT64
postCode         STRING
email         STRING
```

### Wildcards

The `*` operator can be specified at any nesting level to represent all fields at that level. For example, to select all shipping-address fields one would write.

The same is true for wildcard selections. The following:

```
PCollection<Row> rows = input.apply(Select.fieldNames("shippingAddress.*"));
```

Will result in the following schema:

```
Field Name         Field Type
streetAddress      STRING
city               STRING
state              nullable STRING
country            STRING
postCode           STRING

```

### Select array

When selecting fields nested inside of an array, the same rule applies that each selected field appears separately as a top-level field in the resulting row. This means that if multiple fields are selected from the same nested row, each selected field will appear as its own array field.

```
PCollection<Row> rows = input.apply(Select.fieldNames( "transactions.bank", "transactions.purchaseAmount"));
```

Will result in the following schema:

```
Field Name        Field Type
bank              ARRAY[STRING]
purchaseAmount    ARRAY[DOUBLE]
```

### Flatten schema

Another use of the `Select` transform is to flatten a nested schema into a single flat schema.

```
PCollection<Row> rows = input.apply(Select.flattenedSchema());
```

Will result in the following schema:

```
Field Name                          Field Type
userId                              STRING
itemId                              STRING
shippingAddress_streetAddress       STRING
shippingAddress_city                nullable STRING
shippingAddress_state               STRING
shippingAddress_country             STRING
shippingAddress_postCode            STRING
costCents                           INT64
transactions_bank                   ARRAY[STRING]
transactions_purchaseAmount         ARRAY[DOUBLE]

```

### Playground exercise

In the playground window you can find examples of using `Select`.

You can output a field of the first level. And nested at the same time

```
PCollection<Row> game = input.apply(Select.fieldNames("userName","game.*"));
game.apply("User game", ParDo.of(new LogOutput<>("Game")));
```