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

# CoGroup

A transform that performs equijoins across multiple schema PCollections.

This transform has similarities to `CoGroupByKey`, however works on PCollections that have schemas. This allows users of the transform to simply specify schema fields to join on. The output type of the transform is Row that contains one row field for the key and an ITERABLE field for each input containing the rows that joined on that key; by default the cross product is not expanded, but the cross product can be optionally expanded. By default the key field is named "key" (the name can be overridden using `withKeyField`) and has index 0. The tags in the `PCollectionTuple` control the names of the value fields in the `Row`.

For example, the following demonstrates joining three PCollections on the "user" and "country" fields:


```
PCollection<Row> input = PCollectionTuple.of("input1", input1, "input2", input2, "input3", input3)
    .apply(CoGroup.join(By.fieldNames("user", "country")));
```

### JOIN DIFFERENT FIELDS

It's also possible to join between different fields in two inputs, as long as the types of those fields match. In this case, fields must be specified for every input PCollection. For example:

For example, consider the SQL join: `SELECT * FROM input1Tag JOIN input2Tag ON input1Tag.referringUser = input2Tag.user`

```
PCollection input = PCollectionTuple.of("input1Tag", input1, "input2Tag", input2)
   .apply(CoGroup
     .join("input1Tag", By.fieldNames("referringUser")))
     .join("input2Tag", By.fieldNames("user")));
```


### INNER JOIN

For example, consider the SQL join: `SELECT * FROM input1 INNER JOIN input2 ON input1.user = input2.user`

```
PCollection input = PCollectionTuple.of("input1", input1, "input2", input2)
   .apply(CoGroup.join(By.fieldNames("user")).crossProductJoin();
```

### LEFT OUTER JOIN

For example, consider the SQL join: `SELECT * FROM input1 LEFT OUTER JOIN input2 ON input1.user = input2.user`

```
PCollection input = PCollectionTuple.of("input1", input1, "input2", input2)
   .apply(CoGroup.join("input1", By.fieldNames("user").withOptionalParticipation())
                 .join("input2", By.fieldNames("user"))
                 .crossProductJoin();
```

### RIGHT OUTER JOIN

For example, consider the SQL join: `SELECT * FROM input1 RIGHT OUTER JOIN input2 ON input1.user = input2.user`

```
PCollection input = PCollectionTuple.of("input1", input1, "input2", input2)
   .apply(CoGroup.join("input1", By.fieldNames("user"))
                 .join("input2", By.fieldNames("user").withOptionalParticipation())
                 .crossProductJoin();
```

### FULL OUTER JOIN

For example, consider the SQL join: `SELECT * FROM input1 FULL OUTER JOIN input2 ON input1.user = input2.user`

```
PCollection input = PCollectionTuple.of("input1", input1, "input2", input2)
   .apply(CoGroup.join("input1", By.fieldNames("user").withOptionalParticipation())
                 .join("input2", By.fieldNames("user").withOptionalParticipation())
                 .crossProductJoin();
```

### Playground exercise

In the playground window you can find examples of using the `CoGroup`. By running this example, you will see user statistics in certain games.

You can combine several classes

Can you add your **own class** `UserDetails` to have **firstName** field and **lastName**:

```
public static class UserDetails {
        public String userId;
        public String userFirstName;
        public String userLastName;
}
```

You can combine more than two tables:
```
PCollection<UserDetails> details = pipeline.apply(Create.of(new UserDetails("userId","first","last")))
PCollection<Row> coGroupPCollection =
                PCollectionTuple.of("user", userInfo).and("game", gameInfo).and("details", details)
                        .apply(CoGroup.join(CoGroup.By.fieldNames("userId")));
```