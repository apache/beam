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

A generic grouping transform for schema `PCollections`.

When used without a combiner, this transforms simply acts as a `GroupByKey` but without the need for the user to explicitly extract the keys. For example, consider the following input type:

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
PCollection<Row> byUser = purchases.apply(Group.byFieldNames("userId', "country"));
```

### Group with aggregation

However often an aggregation of some form is desired. The builder methods inside the `Group` class allows building up separate aggregations for every field (or set of fields) on the input schema, and generating an output schema based on these aggregations. For example:

```
PCollection<Row> aggregated = purchases
     .apply(Group.byFieldNames("userId', "country")
          .aggregateField("cost", Sum.ofLongs(), "total_cost")
          .aggregateField("cost", Top.<Long>largestLongsFn(10), "top_purchases")
          .aggregateField("cost", ApproximateQuantilesCombineFn.create(21),
              Field.of("transactionDurations", FieldType.array(FieldType.INT64)));
```

The result will be a new row schema containing the fields `total_cost`, ``top_purchases``, and `transactionDurations`, containing the sum of all purchases costs (for that user and country), the top ten purchases, and a histogram of transaction durations. The schema will also contain a key field, which will be a row containing userId and country.

Note that usually the field type can be automatically inferred from the `Combine.CombineFn` passed in. However sometimes it cannot be inferred, due to Java type erasure, in which case a `Schema.Field` object containing the field type must be passed in. This is currently the case for `ApproximateQuantilesCombineFn` in the above example.