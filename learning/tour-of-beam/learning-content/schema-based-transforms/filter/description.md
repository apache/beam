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

# Filter

A `PTransform` for filtering a collection of schema types.

Separate Predicates can be registered for different schema fields, and the result is allowed to pass if all predicates return true. The output type is the same as the input type.

### Single fields filter

For example, consider the following schema type:

```
public class Location {
   public double latitude;
   public double longitude;
}
```

In order to examine only locations in south Manhattan, you would write:

```
PCollection<Location> locations = readLocations();
locations.apply(Filter
   .whereFieldName("latitude", latitude -> latitude < 40.720 && latitude > 40.699)
   .whereFieldName("longitude", longitude -> longitude < -73.969 && longitude > -74.747));
```

### Multiple fields filter

You can also use multiple fields inside the filtering predicate. For example, consider the following schema type representing user account:

```
class UserAccount {
   public double spendOnBooks;
   public double spendOnMovies;
   ...
}
```

Let's say you'd like to process only users who's total spend is over $100. You could write:

```
PCollection<UserAccount> input = readUsers();
input.apply(Filter
    .whereFieldNames(Lists.newArrayList("spendOnBooks", "spendOnMovies"),
        row -> return row.getDouble("spendOnBooks") + row.getDouble("spendOnMovies") > 100.00));
```

### Playground exercise

In the playground window you can find examples of using the `Filter` button.
By running this example, you will see user statistics in certain games.
You can do by multiple fields:
```
.apply(Filter.create().whereFieldNames(Arrays.asList("userId", "score"), new SerializableFunction<Row, Boolean>() {
                    @Override
                    public Boolean apply(Row input) {
                        return input.getString("userId").toLowerCase().startsWith("a") || input.getInt32("score") > 10;
                    }
}))
```