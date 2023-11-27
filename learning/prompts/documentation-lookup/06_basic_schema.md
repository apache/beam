Prompt:
What are schemas in Apache Beam
Response:

 A [Schema in Apache Beam](https://beam.apache.org/documentation/programming-guide/#schemas) is a language-independent type definition for a PCollection. Schema defines elements of that PCollection as an ordered list of named fields.

In many cases, the element type in a PCollection has a structure that can be introspected. Some examples are JSON, Protocol Buffer, Avro, and database row objects. All of these formats can be converted to Beam Schemas.

To take advantage of schemas, your PCollections must have a schema attached. Often, the source itself will attach a schema to the PCollection.

While schemas themselves are language-independent, they are designed to embed naturally into the Beam SDK programming languages. This allows Beam users to continue [using native types](https://beam.apache.org/documentation/programming-guide/#schemas-for-pl-types) while leveraging the advantage of having Beam understand their element schemas.

Beam provides a collection of [schema transforms](https://beam.apache.org/documentation/programming-guide/#662-schema-transforms) that operate natively on schemas. See [SqlTransform](https://beam.apache.org/documentation/dsls/sql/overview/) for an example.

Key capabilities provided by schema transforms include:
* field selection
* grouping and aggregation
* join operations
* filtering data
* adding and removing fields
* renaming fields
* type conversion
* enhanced ParDo functionality

See [Schema Usage Patterns](https://beam.apache.org/documentation/patterns/schema/) for more details and Beam best practices working with structured data.