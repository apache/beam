/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.example.model.Customer;
import org.apache.beam.sdk.extensions.sql.example.model.Order;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * This example uses Beam SQL DSL to query a data pipeline with Java objects in it.
 *
 * <p>Run the example from the Beam source root with
 *
 * <pre>
 *   ./gradlew :sdks:java:extensions:sql:runPojoExample
 * </pre>
 *
 * <p>The above command executes the example locally using direct runner. Running the pipeline in
 * other runners require additional setup and are out of scope of the SQL examples. Please consult
 * Beam documentation on how to run pipelines.
 *
 * <p>This example models a scenario of customers buying goods.
 *
 * <ul>
 *   <li>{@link Customer} represents a customer
 *   <li>{@link Order} represents an order by a customer
 * </ul>
 */
class BeamSqlPojoExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    // First step is to get PCollections of source objects.
    // In this example we create them directly in memory using Create.of().
    //
    // In real world such PCollections will likely be obtained from some other source,
    // e.g. a database or a text file. This process is not specific to Beam SQL,
    // please consult Beam programming guide for details.

    PCollection<Customer> customers = loadCustomers(pipeline);
    PCollection<Order> orders = loadOrders(pipeline);

    // Example 1. Run a simple query over java objects:
    PCollection<Row> customersFromWonderland =
        customers.apply(
            SqlTransform.query(
                "SELECT id, name "
                    + " FROM PCOLLECTION "
                    + " WHERE countryOfResidence = 'Wonderland'"));

    // Output the results of the query:
    customersFromWonderland.apply(logRecords(": is from Wonderland"));

    // Example 2. Query the results of the first query:
    PCollection<Row> totalInWonderland =
        customersFromWonderland.apply(SqlTransform.query("SELECT COUNT(id) FROM PCOLLECTION"));

    // Output the results of the query:
    totalInWonderland.apply(logRecords(": total customers in Wonderland"));

    // Example 3. Query multiple PCollections of Java objects:
    PCollection<Row> ordersByGrault =
        PCollectionTuple.of(new TupleTag<>("customers"), customers)
            .and(new TupleTag<>("orders"), orders)
            .apply(
                SqlTransform.query(
                    "SELECT customers.name, ('order id:' || CAST(orders.id AS VARCHAR))"
                        + " FROM orders "
                        + "   JOIN customers ON orders.customerId = customers.id"
                        + " WHERE customers.name = 'Grault'"));

    // Output the results of the query:
    ordersByGrault.apply(logRecords(": ordered by 'Grault'"));

    pipeline.run().waitUntilFinish();
  }

  private static MapElements<Row, Void> logRecords(String suffix) {
    return MapElements.via(
        new SimpleFunction<Row, Void>() {
          @Override
          public Void apply(Row input) {
            System.out.println(input.getValues() + suffix);
            return null;
          }
        });
  }

  private static PCollection<Customer> loadCustomers(Pipeline pipeline) {
    return pipeline.apply(
        Create.of(
            new Customer(1, "Foo", "Wonderland"),
            new Customer(2, "Bar", "Super Kingdom"),
            new Customer(3, "Baz", "Wonderland"),
            new Customer(4, "Grault", "Wonderland"),
            new Customer(5, "Qux", "Super Kingdom")));
  }

  private static PCollection<Order> loadOrders(Pipeline pipeline) {
    return pipeline.apply(
        Create.of(
            new Order(1, 5),
            new Order(2, 2),
            new Order(3, 1),
            new Order(4, 3),
            new Order(5, 1),
            new Order(6, 5),
            new Order(7, 4),
            new Order(8, 4),
            new Order(9, 1)));
  }
}
