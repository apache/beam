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

# Overview

Most structured records share some common characteristics:

→  They can be subdivided into separate named fields. Fields usually have string names, but sometimes - as in the case of indexed tuples - have numerical indices instead.

→  There is a confined list of primitive types that a field can have. These often match primitive types in most programming languages: int, long, string, etc.

→  Often a field type can be marked as optional (sometimes referred to as nullable) or required.

Often records have a nested structure. A nested structure occurs when a field itself has subfields so the type of the field itself has a schema. Fields that are array or map types is also a common feature of these structured records.

For example, consider the following schema, representing actions in a fictitious e-commerce company:

**Purchase**

```
Field Name              Field Type
userId                  STRING
itemId                  INT64
shippingAddress         ROW(ShippingAddress)
cost                    INT64
transactions            ARRAY[ROW(Transaction)]
```

**ShippingAddress**

```
Field Name              Field Type
streetAddress           STRING
city                    STRING
state                   nullable STRING
country                 STRING
postCode                STRING
```

**Transaction**

```
Field Name              Field Type
bank                    STRING
purchaseAmount          DOUBLE
```

Schemas provide us a type-system for Beam records that is independent of any specific programming-language type. There might be multiple Java classes that all have the same schema (for example a Protocol-Buffer class or a POJO class), and Beam will allow us to seamlessly convert between these types. Schemas also provide a simple way to reason about types across different programming-language APIs.

A `PCollection` with a schema does not need to have a `Coder` specified, as Beam knows how to encode and decode Schema rows; Beam uses a special coder to encode schema types.

### Creating Schemas

While schemas themselves are language independent, they are designed to embed naturally into the programming languages of the Beam SDK being used. This allows Beam users to continue using native types while reaping the advantage of having Beam understand their element schemas.

In Java you could use the following set of classes to represent the purchase schema. Beam will automatically infer the correct schema based on the members of the class.

#### Java POJOs

A `POJO` (Plain Old Java Object) is a Java object that is not bound by any restriction other than the Java Language Specification. A `POJO` can contain member variables that are primitives, that are other POJOs, or are collections maps or arrays thereof. `POJO`s do not have to extend prespecified classes or extend any specific interfaces.

If a `POJO` class is annotated with `@DefaultSchema(JavaFieldSchema.class)`, Beam will automatically infer a schema for this class. Nested classes are supported as are classes with List, array, and Map fields.

For example, annotating the following class tells Beam to infer a schema from this `POJO` class and apply it to any `PCollection<TransactionPojo>`.

```
@DefaultSchema(JavaFieldSchema.class)
public class TransactionPojo {
  public final String bank;
  public final double purchaseAmount;
  @SchemaCreate
  public TransactionPojo(String bank, double purchaseAmount) {
    this.bank = bank;
    this.purchaseAmount = purchaseAmount;
  }
}
// Beam will automatically infer the correct schema for this PCollection. No coder is needed as a result.
PCollection<TransactionPojo> pojos = readPojos();
```
The `@SchemaCreate` annotation tells Beam that this constructor can be used to create instances of `TransactionPojo`, assuming that constructor parameters have the same names as the field names. `@SchemaCreate` can also be used to annotate static factory methods on the class, allowing the constructor to remain private. If there is no @SchemaCreate annotation then all the fields must be non-final and the class must have a zero-argument constructor.

There are a couple of other useful annotations that affect how Beam infers schemas. By default the schema field names inferred will match that of the class field names. However `@SchemaFieldName` can be used to specify a different name to be used for the schema field. @SchemaIgnore can be used to mark specific class fields as excluded from the inferred schema. For example, it’s common to have ephemeral fields in a class that should not be included in a schema (e.g. caching the hash value to prevent expensive recomputation of the hash), and @SchemaIgnore can be used to exclude these fields. Note that ignored fields will not be included in the encoding of these records.

In some cases it is not convenient to annotate the POJO class, for example if the POJO is in a different package that is not owned by the Beam pipeline author. In these cases the schema inference can be triggered programmatically in pipeline’s main function as follows:

```
pipeline.getSchemaRegistry().registerPOJO(TransactionPOJO.class);
```

#### Java Beans

Java Beans are a de-facto standard for creating reusable property classes in Java. While the full standard has many characteristics, the key ones are that all properties are accessed via getter and setter classes, and the name format for these getters and setters is standardized. A Java Bean class can be annotated with `@DefaultSchema(JavaBeanSchema.class)` and Beam will automatically infer a schema for this class.

The `@SchemaCreate` annotation can be used to specify a constructor or a static factory method, in which case the setters and zero-argument constructor can be omitted.

```
@DefaultSchema(JavaBeanSchema.class)
public class Purchase {
  public String getUserId();  // Returns the id of the user who made the purchase.
  public long getItemId();  // Returns the identifier of the item that was purchased.
  public ShippingAddress getShippingAddress();  // Returns the shipping address, a nested type.
  public long getCostCents();  // Returns the cost of the item.
  public List<Transaction> getTransactions();  // Returns the transactions that paid for this purchase (returns a list, since the purchase might be spread out over multiple credit cards).

  @SchemaCreate
  public Purchase(String userId, long itemId, ShippingAddress shippingAddress, long costCents, List<Transaction> transactions) {
      ...
  }
}
```

`@SchemaFieldName` and `@SchemaIgnore` can be used to alter the schema inferred, just like with `POJO` classes.

#### AutoValue

Java value classes are notoriously difficult to generate correctly. There is a lot of boilerplate you must create in order to properly implement a value class. `AutoValue` is a popular library for easily generating such classes by implementing a simple abstract base class.

Beam can infer a schema from an `AutoValue` class. For example:

```
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ShippingAddress {
  public abstract String streetAddress();
  public abstract String city();
  public abstract String state();
  public abstract String country();
  public abstract String postCode();
}
```

This is all that’s needed to generate a simple `AutoValue` class, and the above `@DefaultSchema` annotation tells Beam to infer a schema from it. This also allows AutoValue elements to be used inside of `PCollections`.

`@SchemaFieldName` and `@SchemaIgnore` can be used to alter the schema inferred.

### Playground exercise

You can find the complete code of this example in the playground window you can run and experiment with.

One of the differences you will notice is that it also contains the part to output `PCollection` elements to the console.

Do you also notice in what order elements of PCollection appear in the console? Why is that? You can also run the example several times to see if the output stays the same or changes.