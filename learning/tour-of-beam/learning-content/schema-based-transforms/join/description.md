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

# Joins

A transform that performs equijoins across two schema PCollections.

This transform allows joins between two input `PCollections` simply by specifying the fields to join on. The resulting `PCollection<Row>` will have two fields named "**lhs**" and "**rhs**" respectively, each with the schema of the corresponding input `PCollection`.

For example, the following demonstrates joining two `PCollections` using a natural join on the "**user**" and "**country**" fields, where both the left-hand and the right-hand `PCollections` have fields with these names.

```
PCollection<Row> joined = input1.apply(Join.innerJoin(input2).using("user", "country"));
```

If the right-hand `PCollection` contains fields with different names to join against, you can specify them as follows:

```
PCollection<Row> joined = input1.apply(Join.innerJoin(input2)
       .on(FieldsEqual.left("user", "country").right("otherUser", "otherCountry")));
```

### Supported methods

* `Full outer join`
* `Left outer join`
* `Right outer join`
* `Inner join`
* `Left inner join`
* `Right inner join`


### Playground exercise

In the playground window you can find examples of using the `Join`. By running this example, you will see user statistics in certain games.

You can use other joins simply by changing the function name:
```
.apply(Join.fullOuterJoin(gameInfo).using("userId"));
```