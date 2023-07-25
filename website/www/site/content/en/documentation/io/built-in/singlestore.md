---
title: "Apache SingleStore I/O connector"
---
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

[Built-in I/O Transforms](/documentation/io/built-in/)

# SingleStoreDB I/O
Pipeline options and general information about using and running SingleStoreDB I/O.

## Before you start

To use SingleStoreDB I/O, add the Maven artifact dependency to your `pom.xml` file.

{{< highlight >}}
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-singlestore</artifactId>
    <version>{{< param release_latest >}}</version>
</dependency>
{{< /highlight >}}

Additional resources:

* [SingleStoreIO source code](https://github.com/apache/beam/tree/master/sdks/java/io/singlestore/src/main/java/org/apache/beam/sdk/io/singlestore)
* [SingleStoreIO Javadoc](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/singlestore/SingleStoreIO.html)
* [SingleStore documentation](https://docs.singlestore.com/)

## Authentication
DataSource configuration is required for configuring SingleStoreIO connection properties.

Create the DataSource configuration:
{{< highlight >}}
SingleStoreIO.DataSourceConfiguration
    .create("myHost:3306")
    .withDatabase("db")
    .withConnectionProperties("connectTimeout=30000;useServerPrepStmts=FALSE")
    .withPassword("password")
    .withUsername("admin");
{{< /highlight >}}

Where parameters can be:

- `.create(endpoint)`
    - Hostname or IP address of the SingleStoreDB in the form host:[port] (port is optional).
    - Required parameter.
    - Example: `.create("myHost:3306")`.
- `.withUsername(username)`
    - SingleStoreDB username.
    - Default - `root`.
    - Example: `.withUsername("USERNAME")`.
- `.withPassword(password)`
    - Password of the SingleStoreDB user.
    - Default - empty String.
    - Example: `.withPassword("PASSWORD")`.
- `.withDatabase(database)`
    - Name of the SingleStoreDB database to use.
    - Example: `.withDatabase("MY_DATABASE")`.
- `.withConnectionProperties(connectionProperties)`
    - List of properties that are used by JDBC Driver.
    - The format is “key1=value1;key2=value2;...”.
    - A full list of supported properties can be found [here](https://docs.singlestore.com/managed-service/en/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver.html#connection-string-parameters).
    - Example: `.withConnectionProperties("connectTimeout=30000;useServerPrepStmts=FALSE")`.


**Note** - `.withDatabase(...)` **is required for `.readWithPartitions()`**.

## Reading from SingleStoreDB
One of the functions of SingleStoreIO is reading from SingleStoreDB tables.
SingleStoreIO supports two types of reading:
 - Sequential data reading (`.read()`)
 - Parallel data reading (`.readWithPartitions()`)

In many cases, parallel data reading is preferred over sequential data reading because of performance reasons.

### Sequential data reading
The basic `.read()` operation usage is as follows:
{{< highlight >}}
PCollection<USER_DATA_TYPE> items = pipeline.apply(
    SingleStoreIO.<USER_DATA_TYPE>read()
        .withDataSourceConfiguration(dc)
        .withTable("MY_TABLE") // or .withQuery("QUERY")
        .withStatementPreparator(statementPreparator)
        .withOutputParallelization(true)
        .withRowMapper(mapper)
);
{{< /highlight >}}

Where parameters can be:

- `.withDataSourceConfiguration(dataSourceConfiguration)`
    - `DataSourceConfiguration` object with all information needed to establish a connection to the database. See [authentication](#authentication) for more information.
    - Required parameter.
- `.withTable(table)`
    - Table to read data from.
    - Example: `.withTable("MY_TABLE")`.
- `.withQuery(query)`
    - SQL query to execute.
    - Example: `.withTable("SELECT * FROM MY_TABLE")`.
- `.withStatementPreparator(statementPreparator)`
    - [StatementPreparator](#statementpreparator) object.
- `.withRowMapper(rowMapper)`
    - [RowMapper](#rowmapper) object.
    - Required parameter.
- `.withOutputParallelization(outputParallelization)`
    - Boolean value that indicates whether to reshuffle the result.
    - Default - `true`.
    - Example: `.withOutputParallelization(true)`.

**Note** - either `.withTable(...)` or `.withQuery(...)` **is required**.

### Parallel data reading
The basic `.readWithPartitions()` operation usage is as follows:
{{< highlight >}}
PCollection<USER_DATA_TYPE> items = pipeline.apply(
    SingleStoreIO.<USER_DATA_TYPE>readWithPartitions()
        .withDataSourceConfiguration(dc)
        .withTable("MY_TABLE") // or .withQuery("QUERY")
        .withRowMapper(mapper)
);
{{< /highlight >}}

Where parameters can be:

- `.withDataSourceConfiguration(dataSourceConfiguration)`
    - `DataSourceConfiguration` object with all information needed to establish a connection to the database. See [DataSource Configuration](#authentication) for more information.
    - Required parameter.
- `.withTable(table)`
    - Table to read data from.
    - Example: `.withTable("MY_TABLE")`.
- `.withQuery(query)`
    - SQL query to execute.
    - Example: `.withTable("SELECT * FROM MY_TABLE")`.
- `.withRowMapper(rowMapper)`
    - [RowMapper](#rowmapper) object.
    - Required parameter.

**Note** - either `.withTable(...)` or `.withQuery(...)` **is required**.

### StatementPreparator
The `StatementPreparator` is used by `read()` to set the parameters of the `PreparedStatement`.
For example:
{{< highlight >}}
public static class MyStatmentPreparator implements SingleStoreIO.StatementPreparator {
    @Override
    public void setParameters(PreparedStatement preparedStatement) throws Exception {
        preparedStatement.setInt(1, 10);
    }
}
{{< /highlight >}}

### RowMapper
The `RowMapper` is used by `read()` and `readWithPartitions()` for converting each row of the `ResultSet`
into an element of the resulting `PCollection`.
For example:
{{< highlight >}}
public static class MyRowMapper implements SingleStoreIO.RowMapper<MyRow> {
    @Override
    public MyRow mapRow(ResultSet resultSet) throws Exception {
        return MyRow.create(resultSet.getInt(1), resultSet.getString(2));
    }
}
{{< /highlight >}}

## Writing to SingleStoreDB tables
One of the functions of SingleStoreIO is writing to SingleStoreDB tables.
This transformation enables you to send the user's [PCollection](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/PCollection.html) to your SingleStoreDB database.
It returns number of rows written by each batch of elements.

The basic `.write()` operation usage is as follows:
{{< highlight >}}
data.apply(
    SingleStoreIO.<USER_DATA_TYPE>write()
        .withDataSourceConfiguration(dc)
        .withTable("MY_TABLE")
        .withUserDataMapper(mapper)
        .withBatchSize(100000)
);
{{< /highlight >}}

Where parameters can be:

- `.withDataSourceConfiguration(dataSourceConfiguration)`
    - `DataSourceConfiguration` object with all information needed to establish a connection to the database. See [DataSource Configuration](#authentication) for more information.
    - Required parameter.
- `.withTable(table)`
    - Table in which data should be saved.
    - Required parameter.
    - Example: `.withTable("MY_TABLE")`.
- `.withBatchSize(batchSize)`
    - Number of rows loaded by one `LOAD DATA` query.
    - Default - 100000.
    - Example: `.withBatchSize(100000)`.
- `.withUserDataMapper(userDataMapper)`
    - [UserDataMapper](#userdatamapper) object.
    - Required parameter.

### UserDataMapper
The `UserDataMapper` is required to map data from a `PCollection` to an array of `String` values before the `write()` operation saves the data.
For example:
{{< highlight >}}
public static class MyRowDataMapper implements SingleStoreIO.UserDataMapper<MyRow> {
    @Override
    public List<String> mapRow(MyRow element) {
        List<String> res = new ArrayList<>();
        res.add(element.id().toString());
        res.add(element.name());
        return res;
    }
}
{{< /highlight >}}
