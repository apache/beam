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
# Logical types

Users can extend the schema type system to add custom logical types that can be used as a field. A logical type is identified by a unique identifier and an argument. A logical type also specifies an underlying schema type to be used for storage, along with conversions to and from that type. As an example, a logical union can always be represented as a row with nullable fields, where the user ensures that only one of those fields is ever set at a time.

In Java, a logical type is specified as a subclass of the LogicalType class. A custom Java class can be specified to represent the logical type and conversion functions must be supplied to convert back and forth between this Java class and the underlying Schema type representation. For example, the logical type representing nanosecond timestamp might be implemented as follows

```
// A Logical type using java.time.Instant to represent the logical type.
public class TimestampNanos implements LogicalType<Instant, Row> {
  // The underlying schema used to represent rows.
  private final Schema SCHEMA = Schema.builder().addInt64Field("seconds").addInt32Field("nanos").build();
  @Override public String getIdentifier() { return "timestampNanos"; }
  @Override public FieldType getBaseType() { return schema; }

  // Convert the representation type to the underlying Row type. Called by Beam when necessary.
  @Override public Row toBaseType(Instant instant) {
    return Row.withSchema(schema).addValues(instant.getEpochSecond(), instant.getNano()).build();
  }

  // Convert the underlying Row type to an Instant. Called by Beam when necessary.
  @Override public Instant toInputType(Row base) {
    return Instant.of(row.getInt64("seconds"), row.getInt32("nanos"));
  }

     ...
}
```

### EnumerationType

This logical type allows creating an enumeration type consisting of a set of named constants.

```
Schema schema = Schema.builder()
               …
     .addLogicalTypeField("color", EnumerationType.create("RED", "GREEN", "BLUE"))
     .build();
```

The value of this field is stored in the row as an `INT32` type, however the logical type defines a value type that lets you access the enumeration either as a string or a value. For example:

```
EnumerationType.Value enumValue = enumType.valueOf("RED");
enumValue.getValue();  // Returns 0, the integer value of the constant.
enumValue.toString();  // Returns "RED", the string value of the constant
```

Given a row object with an enumeration field, you can also extract the field as the enumeration value.

```
EnumerationType.Value enumValue = row.getLogicalTypeValue("color", EnumerationType.Value.class);
```

### OneOfType

OneOfType allows creating a disjoint union type over a set of schema fields. For example:

```
Schema schema = Schema.builder()
               …
     .addLogicalTypeField("oneOfField",
        OneOfType.create(Field.of("intField", FieldType.INT32),
                         Field.of("stringField", FieldType.STRING),
                         Field.of("bytesField", FieldType.BYTES)))
      .build();
```

The value of this field is stored in the row as another `Row` type, where all the fields are marked as nullable. The logical type however defines a `Value` object that contains an enumeration value indicating which field was set and allows getting just that field:

```
// Returns an enumeration indicating all possible case values for the enum.
// For the above example, this will be
// EnumerationType.create("intField", "stringField", "bytesField");
EnumerationType oneOfEnum = onOfType.getCaseEnumType();

// Creates an instance of the union with the string field set.
OneOfType.Value oneOfValue = oneOfType.createValue("stringField", "foobar");

// Handle the oneof
switch (oneOfValue.getCaseEnumType().toString()) {
  case "intField":
    return processInt(oneOfValue.getValue(Integer.class));
  case "stringField":
    return processString(oneOfValue.getValue(String.class));
  case "bytesField":
    return processBytes(oneOfValue.getValue(bytes[].class));
}
```

