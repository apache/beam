Prompt:
What are schemas in Apache Beam?

Response:
A [Schema in Apache Beam](https://beam.apache.org/documentation/programming-guide/#schemas) is a language-independent type definition for a `PCollection`. The schema defines elements of that `PCollection` as an ordered list of named fields.

In many cases, the element type in a `PCollection` has a structure that can be introspected. Some examples are JSON, Protocol Buffer, Avro, and database row objects. All of these formats can be converted to Beam Schemas.

To take advantage of schemas, your `PCollection` objects must have a schema attached. Often, the source itself attaches a schema to the `PCollection`.

Although schemas are language-independent, they're designed to embed naturally into the Beam SDK programming languages. This feature lets you continue [to use native types](https://beam.apache.org/documentation/programming-guide/#schemas-for-pl-types) while leveraging the advantage of having Beam understand your element schemas.

Beam provides a collection of [schema transforms](https://beam.apache.org/documentation/programming-guide/#662-schema-transforms) that operate on schemas. For an example, see [SqlTransform](https://beam.apache.org/documentation/dsls/sql/overview/).

The following list includes key capabilities provided by schema transforms:
* field selection
* grouping and aggregation
* join operations
* filtering data
* adding and removing fields
* renaming fields
* type conversion
* enhanced ParDo functionality

For more information about schemas and the Apache Beam best practices when working with structured data, see [Schema Usage Patterns](https://beam.apache.org/documentation/patterns/schema/).
