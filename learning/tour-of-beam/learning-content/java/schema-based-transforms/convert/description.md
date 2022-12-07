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
PCollection<Object> userPCollection = pipeline.apply(Create.of(user1));

// Object convert to Row
PCollection<Row> convertedToRow = userPCollection.apply(Convert.toRows());
```

### Playground exercise

You can find the complete code of this example in the playground window you can run and experiment with.

One of the differences you will notice is that it also contains the part to output `PCollection` elements to the console.

Do you also notice in what order elements of PCollection appear in the console? Why is that? You can also run the example several times to see if the output stays the same or changes.