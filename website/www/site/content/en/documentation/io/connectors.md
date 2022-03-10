# I/O Connectors

Apache Beam I/O connectors provide read and write transforms for the most popular data storage systems so that Beam users can benefit from native optimised connectivity.  With the available I/Os, Apache Beam pipelines can read and write data from and to an external storage type in a unified and distributed way.

### Built-in I/O Connectors

This table provides a consolidated, at-a-glance overview of the available built-in I/O connectors.


<table class="table table-bordered">
  <tr>
    <th>Connector Name</th>
    <th>Source Supported</th>
    <th>Sink Supported</th>
    <th>Java</th>
    <th>Python</th>
    <th>Go</th>
    <th>Batch Supported</th>
    <th>Streaming Supported</th>
  </tr>
  <tr>
    <td>FileIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/2.36.0/org/apache/beam/sdk/io/FileIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/2.36.0/apache_beam.io.fileio.html">native</a>
    </td>
    <td>via X-language</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>AvroIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/2.36.0/org/apache/beam/sdk/io/AvroIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/2.36.0/apache_beam.io.avroio.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio">native</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>TextIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="active">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/2.36.0/org/apache/beam/sdk/io/TextIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/2.36.0/apache_beam.io.textio.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio">native</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>TFRecordIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="active">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/2.36.0/org/apache/beam/sdk/io/TFRecordIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/2.36.0/apache_beam.io.tfrecordio.html">native</a>
    </td>
    <td>via X-language</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
</table>

