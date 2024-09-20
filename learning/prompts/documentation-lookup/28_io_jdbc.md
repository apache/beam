Prompt:
Is Oracle Java Database Connectivity (JDBC) supported in Apache Beam?

Response:
The [Oracle Java Database Connectivity (JDBC) API](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/) is a Java API for connecting to relational databases. Apache Beam supports reading and writing data with JDBC using the `ReadFromJdbc` and `WriteToJdbc` transforms in the `JdbcIO` module. For more information, see the JdbcIO connector documentation for the following languages:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.html)
* [Python (using cross-language transforms)](https://beam.apache.org/releases/pydoc/current/apache_beam.io.jdbc.html)
* [Go (using cross-language transforms)](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/jdbcio)

To get started with JDBC and Apache Beam, refer to the [Dataflow Cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook).

Here is an example of Apache Beam pipeline code for reading data with JDBC:

```python
class ExampleRow(NamedTuple):
    id: int
    name: str

class ReadJdbcOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--driverClassName",
          help="JDBC driver class name")
      parser.add_argument(
          "--url",
          help="JDBC URL")
      parser.add_argument(
          "--username",
          help="JDBC username")
      parser.add_argument(
          "--password",
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
