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
   .whereFieldName("latitude", lat -> lat < 40.720 && lat > 40.699)
   .whereFieldName("longitude", long -> long < -73.969 && long > -74.747));
```

### Multiple fields filter

Predicates that require examining multiple fields at once are also supported. For example, consider the following class representing a user account:

```
class UserAccount {
   public double spendOnBooks;
   public double spendOnMovies;
   ...
}
```

Say you want to examine only users who`s total spend is above $100. You could write:

```
PCollection<UserAccount> users = readUsers();
users.apply(Filter
    .whereFieldNames(Lists.newArrayList("spendOnBooks", "spendOnMovies"),
        row -> return row.getDouble("spendOnBooks") + row.getDouble("spendOnMovies") > 100.00));
```