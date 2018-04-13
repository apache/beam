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
package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.extensions.sql.TestUtils.tuple;

import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.reflect.InferredRowCoder;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for automatic inferring schema from the input {@link PCollection} of pojos.
 */
public class InferredRowCoderSqlTest {

  private static final boolean NOT_NULLABLE = false;
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  /** Person POJO. */
  public static class PersonPojo implements Serializable {
    private Integer ageYears;
    private String name;

    public Integer getAgeYears() {
      return ageYears;
    }

    public String getName() {
      return name;
    }

    PersonPojo(String name, Integer ageYears) {
      this.ageYears = ageYears;
      this.name = name;
    }
  }

  /** Order POJO. */
  public static class OrderPojo implements Serializable {
    private Integer amount;
    private String buyerName;

    public Integer getAmount() {
      return amount;
    }

    public String getBuyerName() {
      return buyerName;
    }

    OrderPojo(String buyerName, Integer amount) {
      this.amount = amount;
      this.buyerName = buyerName;
    }
  }

  @Test
  public void testSelect() {
    PCollection<PersonPojo> input =
        PBegin.in(pipeline).apply(
            "input",
            Create
                .of(
                    new PersonPojo("Foo", 5),
                    new PersonPojo("Bar", 53))
                .withCoder(
                    InferredRowCoder.ofSerializable(PersonPojo.class)));

    String sql = "SELECT name, ageYears FROM PCOLLECTION";

    PCollection<Row> result = input.apply("sql", BeamSql.query(sql));

    PAssert
        .that(result)
        .containsInAnyOrder(
            TestUtils.rowsBuilderOf(
                Schema
                    .builder()
                      .addStringField("name", NOT_NULLABLE)
                      .addInt32Field("ageYears", NOT_NULLABLE)
                      .build())
                .addRows(
                    "Foo", 5,
                    "Bar", 53)
                .getRows());

    pipeline.run();
  }

  @Test
  public void testProject() {
    PCollection<PersonPojo> input =
        PBegin.in(pipeline).apply(
            "input",
            Create
                .of(
                    new PersonPojo("Foo", 5),
                    new PersonPojo("Bar", 53))
                .withCoder(
                    InferredRowCoder.ofSerializable(PersonPojo.class)));

    String sql = "SELECT name FROM PCOLLECTION";

    PCollection<Row> result = input.apply("sql", BeamSql.query(sql));

    PAssert
        .that(result)
        .containsInAnyOrder(
            TestUtils.rowsBuilderOf(
                Schema
                    .builder()
                    .addStringField("name", NOT_NULLABLE)
                    .build())
                .addRows("Foo", "Bar")
                .getRows());

    pipeline.run();
  }

  @Test
  public void testJoin() {
    PCollection<PersonPojo> people =
        PBegin.in(pipeline).apply(
            "people",
            Create
                .of(
                    new PersonPojo("Foo", 5),
                    new PersonPojo("Bar", 53))
                .withCoder(
                    InferredRowCoder.ofSerializable(PersonPojo.class)));

    PCollection<OrderPojo> orders =
        PBegin.in(pipeline).apply(
            "orders",
            Create
                .of(
                    new OrderPojo("Foo", 15),
                    new OrderPojo("Foo", 10),
                    new OrderPojo("Foo", 5),
                    new OrderPojo("Bar", 53),
                    new OrderPojo("Bar", 54),
                    new OrderPojo("Bar", 55))
                .withCoder(
                    InferredRowCoder.ofSerializable(OrderPojo.class)));

    String sql =
        "SELECT name, amount "
            + "FROM buyers INNER JOIN orders "
            + "ON buyerName = name "
            + "WHERE ageYears = 5";

    PCollection<Row> result =
        tuple("buyers", people,
              "orders", orders)
            .apply("sql", BeamSql.query(sql));

    PAssert
        .that(result)
        .containsInAnyOrder(
            TestUtils.rowsBuilderOf(
                Schema
                    .builder()
                    .addStringField("name", NOT_NULLABLE)
                    .addInt32Field("amount", NOT_NULLABLE)
                    .build())
                .addRows(
                    "Foo", 15,
                    "Foo", 10,
                    "Foo", 5)
                .getRows());

    pipeline.run();
  }

  @Test
  public void testAggregation() {
    PCollection<PersonPojo> people =
        PBegin.in(pipeline).apply(
            "people",
            Create
                .of(
                    new PersonPojo("Foo", 5),
                    new PersonPojo("Bar", 53))
                .withCoder(
                    InferredRowCoder.ofSerializable(PersonPojo.class)));

    PCollection<OrderPojo> orders =
        PBegin.in(pipeline).apply(
            "orders",
            Create
                .of(
                    new OrderPojo("Foo", 15),
                    new OrderPojo("Foo", 10),
                    new OrderPojo("Foo", 5),
                    new OrderPojo("Bar", 53),
                    new OrderPojo("Bar", 54),
                    new OrderPojo("Bar", 55))
                .withCoder(
                    InferredRowCoder.ofSerializable(OrderPojo.class)));

    String sql =
        "SELECT name, SUM(amount) as total "
            + "FROM buyers INNER JOIN orders "
            + "ON buyerName = name "
            + "GROUP BY name";

    PCollection<Row> result =
        tuple("buyers", people,
              "orders", orders)
            .apply("sql", BeamSql.query(sql));

    PAssert
        .that(result)
        .containsInAnyOrder(
            TestUtils.rowsBuilderOf(
                Schema
                    .builder()
                    .addStringField("name", NOT_NULLABLE)
                    .addInt32Field("total", NOT_NULLABLE)
                    .build())
                .addRows(
                    "Foo", 30,
                    "Bar", 162)
                .getRows());

    pipeline.run();
  }
}
