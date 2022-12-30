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
Pipeline p = Pipeline.create(options);

CoderRegistry cr = p.getCoderRegistry();
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

For example, consider the following schema type:

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

    public static VendorToPassengerDTO of(final Integer passengerCount, final Integer vendorID) {
        return new VendorToPassengerDTO(passengerCount, vendorID);
    }

    public void setPassengerCount(final Integer passengerCount) {
        this.PassengerCount = passengerCount;
    }

    public void setVendorID(final Integer vendorID) {
        this.VendorID = vendorID;
    }

    public Integer getVendorID() {
        return this.VendorID;
    }

    public Integer getPassengerCount() {
        return this.PassengerCount;
    }

    @Override
    public String toString() {
        return String.format("{\"PassengerCount\":%d,\"VendorID\":%d}", PassengerCount, VendorID);
    }
}
```

Explicit `scheme` for converting to `Row`:
```
final Schema schema = Schema.builder()
                .addInt32Field("PassengerCount")
                .addInt32Field("VendorID")
                .build();
```

`Coder` for `VendorToPassengerDTO`:
```
class CustomCoderSecond extends Coder<VendorToPassengerDTO> {
    final ObjectMapper om = new ObjectMapper();

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
        return om.readValue(serializedDTOs, VendorToPassengerDTO.class);
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

You can find the complete code of this example in the playground window you can run and experiment with.