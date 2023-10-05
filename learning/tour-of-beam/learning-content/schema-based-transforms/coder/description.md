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

# Coder

A `Coder<T>` defines how to encode and decode values of type `T` into byte streams.

> Note that coders are unrelated to parsing or formatting data when interacting with external data sources or sinks. You need to do such parsing or formatting explicitly, using transforms such as `ParDo` or `MapElements`.

The Beam SDK requires a coder for every PCollection in your pipeline. In many cases, Beam can automatically infer the Coder for type in `PCollection` and use predefined coders to perform encoding and decoding. However, in some cases, you will need to specify the Coder explicitly or create a Coder for custom types.

To set the `Coder` for `PCollection`, you need to call `PCollection.setCoder`. You can also get the Coder associated with PCollection using the `PCollection.getCoder` method.

### CoderRegistry

When Beam tries to infer Coder for `PCollection`, it uses mappings stored in the `CoderRegistry` object associated with `PCollection`. You can access the `CoderRegistry` for a given pipeline using the method `Pipeline.getCoderRegistry` or get a coder for a particular type using `CoderRegistry.getCoder`.

Please note that since `CoderRegistry` is associated with each `PCollection`, you can encode\decode the same type differently in different `PCollection`.

The following example demonstrates how to register a coder for a type using `CoderRegistry`:

```
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline pipeline = Pipeline.create(options);

CoderRegistry cr = pipeline.getCoderRegistry();
cr.registerCoder(Integer.class, BigEndianIntegerCoder.class);
```

### Specifying default coder for a type

You can specify the default coder for your custom type by annotating it with `@defaultcoder` annotation. For example:
```
@DefaultCoder(AvroCoder.class)
public class MyCustomDataType {
  ...
}
```


`Coder` classes for compound types are often composed of coder classes for types contains therein. The composition of `Coder` instances into a coder for the compound class is the subject of the `Coder` Provider type, which enables automatic generic composition of Coder classes within the CoderRegistry. See `Coder` Provider and `CoderRegistry` for more information about how coders are inferred.

When you create custom objects and schemas, you need to create a subclass of Coder for your object and implement the following methods:
* `encode` - converting objects to bytes
* `decode` - converting bytes to objects
* `getCoderArguments` - If it is a `Coder` for a parameterized type, returns a list of `Coders` used for each of the parameters, in the same order in which they appear in the type signature of the parameterized type.
* `verifyDeterministic` - throw the `Coder.NonDeterministicException`, if the encoding is not deterministic.

When you get the data and when you paint it as a structure, you will need a `dto` class. In this case, `VendorToPassengerDTO`:

```
@DefaultSchema(JavaFieldSchema.class)
class VendorToPassengerDTO {

    @JsonProperty(value = "PassengerCount")
    Integer PassengerCount;

    @JsonProperty(value = "VendorID")
    Integer VendorID;

    @SchemaCreate
    public VendorToPassengerDTO(Integer passengerCount, Integer vendorID) {
        this.PassengerCount = passengerCount;
        this.VendorID = vendorID;
    }

    // Function for TypeDescription
    public static VendorToPassengerDTO of(final Integer passengerCount, final Integer vendorID) {
        return new VendorToPassengerDTO(passengerCount, vendorID);
    }

    // Setter
    // Getter
    // ToString
}
```

`Pipeline` can't use select, group, and so on, because it doesn't understand the data structure, so we need to write our own `Coder`:

```
class CustomCoderSecond extends Coder<VendorToPassengerDTO> {
    final ObjectMapper objectMapper = new ObjectMapper();

    private static final CustomCoderSecond INSTANCE = new CustomCoderSecond();

    public static CustomCoderSecond of() {
        return INSTANCE;
    }

    @Override
    public void encode(VendorToPassengerDTO dto, OutputStream outStream) throws IOException {
        final String result = dto.toString();
        outStream.write(result.getBytes());
    }

    @Override
    public VendorToPassengerDTO decode(InputStream inStream) throws IOException {
        final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
        return objectMapper.readValue(serializedDTOs, VendorToPassengerDTO.class);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() {
    }
}
```

### Playground exercise

In the playground window you can find examples of using `Coder`. By running this example, you will see user statistics in certain games.

You can add a new `winner` field:
```
public static class Game {
        public String userId;
        public Integer score;
        public String gameId;
        public String date;
        public Boolean winner;
}
```

If the `score` is more than **10**, put the `true` in the `Coder`.

```
String line = user.userId + "," + user.userName + ";" + user.game.score + "," + user.game.gameId + "," + user.game.gameId + "," + user.winner;
```

```
String[] game = params[1].split(",");
int score = game[0];
boolean winner = false;
if(score >= 10){
    winner = true;
}
return new User(user[0], user[1], new Game(user[0], Integer.valueOf(game[0]), game[1], game[2],winner));
```