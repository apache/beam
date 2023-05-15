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

# Group

`Group` transform can be used to group records in `PCollection` by one or several fields in the input schema. You can also apply aggregations to those groupings, which is the most common use of the `Group` transform.

The output of the `Group` transform has a schema with one field corresponding to each aggregation.

When used without a combiner, this transforms simply acts as a `GroupByKey` except that you don't have to explicitly extract keys.

For example, consider the following input schema:
```
public class UserPurchase {
   public String userId;
   public String country;
   public long cost;
   public double transactionDuration;
 }
```

### Group by fields

You can group all purchases by user and country as follows:

```
PCollection<Row> byUser = input.apply(Group.byFieldNames("userId', "country"));
```

### Group with aggregation

You will likely be using grouping to aggregate input data. The builder methods inside the `Group` class allow the creation of separate aggregations for every field (or set of fields) on the input schema and generate an output schema based on these aggregations. For example:

```
PCollection<Row> aggregated = input
     .apply(Group.byFieldNames("userId', "country")
          .aggregateField("cost", Sum.ofLongs(), "total_cost")
          .aggregateField("cost", Top.<Long>largestLongsFn(10), "top_purchases")
          .aggregateField("cost", ApproximateQuantilesCombineFn.create(21),
              Field.of("transactionDurations", FieldType.array(FieldType.INT64)));
```

The result will be a new row schema containing the fields **total_cost**, **top_purchases**, and **transactionDurations**, containing the sum of all purchases costs (for that user and country), the top ten purchases, and a histogram of transaction durations. The schema will also contain a key field, a row containing userId and country.

> Note that usually, the field type can be automatically inferred from the `Combine.CombineFn` passed in. However, sometimes it cannot be inferred due to Java type erasure. In such case, you need to specify the field type using `Schema.Field`. In the above example, the type is explicitly specified for the `transactionDurations` field.

### Playground exercise

In the playground window you can find examples of using the `Group`. By running this example, you will see user statistics in certain games.

Instead of `Sum`, you can use other `CombineFn` functions:
```
.apply(Group.byFieldNames("userName").aggregateField("score", Max.ofIntegers(), "total"))
```