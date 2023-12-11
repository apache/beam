Prompt:
Is JDBC supported in Apache Beam?
Response:
[Java Database Connectivity (JDBC)](https://en.wikipedia.org/wiki/Java_Database_Connectivity) is a Java API for connecting to relational databases. Apache Beam supports reading and writing data from JDBC using the `ReadFromJdbc` and `WriteToJdbc` transforms from a `JdbcIO` module:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.html),
* [Python via X-language](https://beam.apache.org/releases/pydoc/current/apache_beam.io.jdbc.html).
* [Go via X-language](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/jdbcio)

[Dataflow-cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) will help you to get started with Avro and Apache Beam.

Apache Beam pipeline code for reading data from JDBC might look like the following example:
```python
class ReadJdbcOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--driverClassName",
          default="org.postgresql.Driver",
          help="JDBC driver class name")
      parser.add_argument(
          "--url",
          default="jdbc:postgresql://localhost:5432/postgres",
          help="JDBC URL")
      parser.add_argument(
          "--username",
          default="postgres",
          help="JDBC username")
      parser.add_argument(
          "--password",
          default="postgres",
          help="JDBC password")
      parser.add_argument(
          "--query",
          default="SELECT * FROM users",
          help="JDBC query")

options = ReadJdbcOptions()

with beam.Pipeline(options=options) as p:

  (p | "Read from JDBC" >> ReadFromJdbc(
      driverClassName=options.driverClassName,
      url=options.url,
      username=options.username,
      password=options.password,
      query=options.query)
     | Map(logging.info))
```

