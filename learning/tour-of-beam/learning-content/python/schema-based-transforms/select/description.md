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

### Top-level fields

In order to select a field at the top level of a schema, the name of the field is specified. For example, to select just the user ids from a `PCollection` of purchases one would write (using the Select transform).

```
PCollection<Row> rows = purchases.apply(Select.fieldNames("userId", "shippingAddress.postCode"));
```

Will result in the following schema:

```
Field Name	   Field Type
userId	           STRING
```

### Nested fields

Individual nested fields can be specified using the dot operator. For example, to select just the postal code from the shipping address one would write.

```
PCollection<Row> rows = purchases.apply(Select.fieldNames("shippingAddress.postCode"));
```

Will result in the following schema:

```
Field Name	   Field Type
postCode	       STRING
```

### Wildcards

The `*` operator can be specified at any nesting level to represent all fields at that level. For example, to select all shipping-address fields one would write.

The same is true for wildcard selections. The following:

```
PCollection<Row> rows = purchases.apply(Select.fieldNames("shippingAddress.*"));
```

Will result in the following schema:

```
Field Name	   Field Type
streetAddress	   STRING
city	           STRING
state	  nullable STRING
country	           STRING
postCode	       STRING

```

### Select array

When selecting fields nested inside of an array, the same rule applies that each selected field appears separately as a top-level field in the resulting row. This means that if multiple fields are selected from the same nested row, each selected field will appear as its own array field.

```
PCollection<Row> rows = purchases.apply(Select.fieldNames( "transactions.bank", "transactions.purchaseAmount"));
```

Will result in the following schema:

```
Field Name	   Field Type
bank	          ARRAY[STRING]
purchaseAmount	  ARRAY[DOUBLE]
```

### Flatten schema

Another use of the `Select` transform is to flatten a nested schema into a single flat schema.

```
PCollection<Row> rows = purchases.apply(Select.flattenedSchema());
```

Will result in the following schema:

```
Field Name	                    Field Type
userId	                            STRING
itemId	                            STRING
shippingAddress_streetAddress	    STRING
shippingAddress_city	   nullable STRING
shippingAddress_state	            STRING
shippingAddress_country	            STRING
shippingAddress_postCode	        STRING
costCents	INT64
transactions_bank	         ARRAY[STRING]
transactions_purchaseAmount	 ARRAY[DOUBLE]

```