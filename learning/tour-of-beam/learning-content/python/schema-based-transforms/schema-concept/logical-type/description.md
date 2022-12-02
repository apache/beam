# Logical types

Users can extend the schema type system to add custom logical types that can be used as a field. A logical type is identified by a unique identifier and an argument. A logical type also specifies an underlying schema type to be used for storage, along with conversions to and from that type. As an example, a logical union can always be represented as a row with nullable fields, where the user ensures that only one of those fields is ever set at a time.

```
```