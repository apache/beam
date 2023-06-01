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
### Converting between types

As mentioned, Beam can automatically convert between different Java types, as long as those types have equivalent schemas. One way to do this is by using the ```Convert``` transform, as follows.

```
PCollection<Object> input = pipeline.apply(Create.of(user1));

// Object convert to Row
PCollection<Row> convertedToRow = input.apply(Convert.toRows());
```

### Playground exercise

In the playground window you can find examples of using the `Convert`. By running this example, you will see user statistics in certain games.
You can add schema with one function:

```
PCollection<Row> userRow = fullStatistics
                .apply(Convert.toRows())
                .setRowSchema(type)
                .apply("User", ParDo.of(new LogOutput<>("ToRows")));
```