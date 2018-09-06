---
layout: section
title: "Beam SQL: Walkthrough"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/walkthrough/
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

# Beam SQL Walkthrough

This page illustrates the usage of Beam SQL with example code.

## Row

Before applying a SQL query to a `PCollection`, the data in the collection must
be in `Row` format. A `Row` represents a single, immutable record in a Beam SQL
`PCollection`. The names and types of the fields/columns in the row are defined
by its associated [RowType]({{ site.baseurl }}/documentation/sdks/javadoc/{{
site.release_latest }}/index.html?org/apache/beam/sdk/values/RowType.html).
For SQL queries, you should use the [RowSqlType.builder()]({{ site.baseurl
}}/documentation/sdks/javadoc/{{ site.release_latest
}}/index.html?org/apache/beam/sdk/extensions/sql/RowSqlType.html) to create
`RowTypes`, it allows creating schemas with all supported SQL types (see [Data
Types]({{ site.baseurl }}/documentation/dsls/sql/data-types) for more details on supported primitive data types).


A `PCollection<Row>` can be obtained multiple ways, for example:

  - **From in-memory data** (typically for unit testing).

    **Note:** you have to explicitly specify the `Row` coder. In this example we're doing it by calling `Create.of(..).withCoder()`:

    ```java
    // Define the record type (i.e., schema).
    RowType appType = 
        RowSqlType
          .builder()
          .withIntegerField("appId")
          .withVarcharField("description")
          .withTimestampField("rowtime")
          .build();

    // Create a concrete row with that type.
    Row row = 
        Row
          .withRowType(appType)
          .addValues(1, "Some cool app", new Date())
          .build();

    // Create a source PCollection containing only that row
    PCollection<Row> testApps = 
        PBegin
          .in(p)
          .apply(Create
                    .of(row)
                    .withCoder(appType.getRowCoder()));
    ```
  - **From a `PCollection<T>` of records of some other type**  (i.e.  `T` is not already a `Row`), by applying a `ParDo` that converts input records to `Row` format.

    **Note:** you have to manually set the coder of the result by calling `setCoder(appType.getRowCoder())`:
    ```java
    // An example POJO class.
    class AppPojo {
      Integer appId;
      String description;
      Date timestamp;
    }

    // Acquire a collection of POJOs somehow.
    PCollection<AppPojo> pojos = ...

    // Convert them to Rows with the same schema as defined above via a DoFn.
    PCollection<Row> apps = pojos
      .apply(
          ParDo.of(new DoFn<AppPojo, Row>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              // Get the current POJO instance
              AppPojo pojo = c.element();

              // Create a Row with the appType schema 
              // and values from the current POJO
              Row appRow = 
                    Row
                      .withRowType(appType)
                      .addValues(
                        pojo.appId, 
                        pojo.description, 
                        pojo.timestamp)
                      .build();

              // Output the Row representing the current POJO
              c.output(appRow);
            }
          }))
      .setCoder(appType.getRowCoder());
    ```

  - **As an output of another `BeamSql` query**. Details in the next section.

Once you have a `PCollection<Row>` in hand, you may use the `BeamSql` APIs to apply SQL queries to it.

## BeamSql transform

`BeamSql.query(queryString)` method is the only API to create a `PTransform`
from a string representation of the SQL query. You can apply this `PTransform`
to either a single `PCollection` or a `PCollectionTuple` which holds multiple
`PCollections`:

  - when applying to a single `PCollection` it can be referenced via the table name `PCOLLECTION` in the query:
    ```java
    PCollection<Row> filteredNames = testApps.apply(
        BeamSql.query(
          "SELECT appId, description, rowtime "
            + "FROM PCOLLECTION "
            + "WHERE id=1"));
    ```
  - when applying to a `PCollectionTuple`, the tuple tag for each `PCollection` in the tuple defines the table name that may be used to query it. Note that table names are bound to the specific `PCollectionTuple`, and thus are only valid in the context of queries applied to it.  

    For example, you can join two `PCollections`:  
    ```java
    // Create the schema for reviews
    RowType reviewType = 
        RowSqlType.
          .withIntegerField("appId")
          .withIntegerField("reviewerId")
          .withFloatField("rating")
          .withTimestampField("rowtime")
          .build();
    
    // Obtain the reviews records with this schema
    PCollection<Row> reviewsRows = ...

    // Create a PCollectionTuple containing both PCollections.
    // TupleTags IDs will be used as table names in the SQL query
    PCollectionTuple namesAndFoods = PCollectionTuple.of(
        new TupleTag<>("Apps"), appsRows), // appsRows from the previous example
        new TupleTag<>("Reviews"), reviewsRows));

    // Compute the total number of reviews 
    // and average rating per app 
    // by joining two PCollections
    PCollection<Row> output = namesAndFoods.apply(
        BeamSql.query(
            "SELECT Names.appId, COUNT(Reviews.rating), AVG(Reviews.rating)"
                + "FROM Apps INNER JOIN Reviews ON Apps.appId == Reviews.appId"));
    ```

[BeamSqlExample](https://github.com/apache/beam/blob/master/sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/example/BeamSqlExample.java)
in the code repository shows basic usage of both APIs.

