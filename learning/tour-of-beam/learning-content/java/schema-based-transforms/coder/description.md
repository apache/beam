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

A Coder<T> defines how to encode and decode values of type T into byte streams.

Coder instances are serialized during job creation and deserialized before use. This will generally be performed by serializing the object via Java Serialization.

Coder classes for compound types are often composed of coder classes for types contains therein. The composition of Coder instances into a coder for the compound class is the subject of the Coder Provider type, which enables automatic generic composition of Coder classes within the CoderRegistry. See Coder Provider and CoderRegistry for more information about how coders are inferred.

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