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

* They can be subdivided into separate named fields. Fields usually have string names, but sometimes - as in the case of indexed tuples - have numerical indices instead.

* There is a confined list of primitive types that a field can have. These often match primitive types in most programming languages: int, long, string, etc.

* Often a field type can be marked as optional (sometimes referred to as nullable) or required.

Often records have a nested structure. A nested structure occurs when a field itself has subfields, so the type of the field itself has a schema. Fields that are array or map types are also a common feature of these structured records.

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
Purchase                ROW(Purchase)
```

**Transaction**

```
Field Name              Field Type
bank                    STRING
purchase                ROW(Purchase)
```

Schemas provide us with a type system for Beam records that is independent of any specific programming-language type. There might be multiple types of Java objects that all have the same schema. For example, you can implement the same schema as Protocol-Buffer or POJO class.

Schemas also provide a simple way to reason about types across different programming-language APIs.

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
The `@SchemaCreate` annotation indicates to Beam that instances of the `TransactionPojo` class can be created using the annotated constructor, as long as the constructor parameters have the same names as the field names. Additionally, this annotation can also be applied to static factory methods on the class, even if the constructor is private. In the absence of the `@SchemaCreate` annotation, all fields must be non-final and the class must have a zero-argument constructor.

A couple of other useful annotations affect how Beam infers schemas. By default, the schema field names will match that of the class field names. However, `@SchemaFieldName` can be used to specify a different name to be used for the schema field.

You can use `@SchemaIgnore` to mark specific class fields as excluded from the inferred schema. For example, it’s common to have ephemeral fields in a class that should not be included in a schema (e.g., caching the hash value to prevent expensive recomputation of the hash), and `@SchemaIgnore` allows to exclude such fields. Note that ignored fields will be excluded from encoding as well.

In some cases it is not convenient to annotate the POJO class, for example if the POJO is in a different package that is not owned by the Beam pipeline author. In these cases the schema inference can be triggered programmatically in pipeline’s main function as follows:

```
pipeline.getSchemaRegistry().registerPOJO(TransactionPOJO.class);
```

#### Java Beans

Java Beans are a de-facto standard for creating reusable property classes in Java. While the full standard has many characteristics, the key ones are that all fields must be accessed using getters and setters, and the name format for these getters and setters is standardized. A Java Bean class can be annotated with `@DefaultSchema(JavaBeanSchema.class)`, and Beam will automatically infer a schema for this class.

Similarly to POJO classes, you can use `@SchemaCreate` annotation to specify a constructor or a static factory method. Otherwise, Beam will use zero arguments constructor and setters to instantiate the class.

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

Java value classes are notoriously difficult to generate correctly. This is because there are a lot of boilerplates you must create to implement a value class properly. AutoValue is a popular library to simplify simple class creation.
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

This is all that’s needed to generate a simple `AutoValue` class, and the above `@DefaultSchema` annotation tells Beam to infer a schema from it. This also allows `AutoValue` elements to be used inside of `PCollections`.

You can also use `@SchemaFieldName` and `@SchemaIgnore` annotations to specify different schema field names or ignore fields, respectively.

### Playground exercise

In the playground window you can find examples of creating schemes. By running this example, you will see user statistics in certain games.
For using `@DefaultSchema(JavaBeanSchema.class)` need getter and setter:
```
@DefaultSchema(JavaBeanSchema.class)
public static class Game {
        public String userId;
        public String score;
        public String gameId;
        public String date;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getScore() {
            return score;
        }

        public void setScore(String score) {
            this.score = score;
        }

        public String getGameId() {
            return gameId;
        }

        public void setGameId(String gameId) {
            this.gameId = gameId;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }
}
```