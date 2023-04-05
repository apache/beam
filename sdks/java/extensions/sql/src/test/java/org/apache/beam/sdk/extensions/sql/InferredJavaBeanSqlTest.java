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
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;

/** Tests for automatic inferring schema from the input {@link PCollection} of JavaBeans. */
public class InferredJavaBeanSqlTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  /** Person Bean. */
  @DefaultSchema(JavaBeanSchema.class)
  public static class PersonBean implements Serializable {
    private Integer ageYears;
    private String name;

    public PersonBean() {}

    public Integer getAgeYears() {
      return ageYears;
    }

    public String getName() {
      return name;
    }

    PersonBean(String name, Integer ageYears) {
      this.ageYears = ageYears;
      this.name = name;
    }

    public void setAgeYears(Integer ageYears) {
      this.ageYears = ageYears;
    }

    public void setName(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PersonBean that = (PersonBean) o;
      return Objects.equals(ageYears, that.ageYears) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ageYears, name);
    }
  }

  /** Order JavaBean. */
  @DefaultSchema(JavaBeanSchema.class)
  public static class OrderBean implements Serializable {
    private Integer amount;
    private String buyerName;

    public OrderBean() {}

    public Integer getAmount() {
      return amount;
    }

    public String getBuyerName() {
      return buyerName;
    }

    OrderBean(String buyerName, Integer amount) {
      this.amount = amount;
      this.buyerName = buyerName;
    }

    public void setAmount(Integer amount) {
      this.amount = amount;
    }

    public void setBuyerName(String buyerName) {
      this.buyerName = buyerName;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OrderBean orderBean = (OrderBean) o;
      return Objects.equals(amount, orderBean.amount)
          && Objects.equals(buyerName, orderBean.buyerName);
    }

    @Override
    public int hashCode() {

      return Objects.hash(amount, buyerName);
    }
  }

  @Test
  public void testSelect() {
    PCollection<PersonBean> input =
        PBegin.in(pipeline)
            .apply("input", Create.of(new PersonBean("Foo", 5), new PersonBean("Bar", 53)));

    String sql = "SELECT name, ageYears FROM PCOLLECTION";

    PCollection<Row> result = input.apply("sql", SqlTransform.query(sql));

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.rowsBuilderOf(
                    Schema.builder().addStringField("name").addInt32Field("ageYears").build())
                .addRows(
                    "Foo", 5,
                    "Bar", 53)
                .getRows());

    pipeline.run();
  }

  @Test
  public void testProject() {
    PCollection<PersonBean> input =
        PBegin.in(pipeline)
            .apply("input", Create.of(new PersonBean("Foo", 5), new PersonBean("Bar", 53)));

    String sql = "SELECT name FROM PCOLLECTION";

    PCollection<Row> result = input.apply("sql", SqlTransform.query(sql));

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.rowsBuilderOf(Schema.builder().addStringField("name").build())
                .addRows("Foo", "Bar")
                .getRows());

    pipeline.run();
  }

  @Test
  public void testJoin() {
    PCollection<PersonBean> people =
        PBegin.in(pipeline)
            .apply("people", Create.of(new PersonBean("Foo", 5), new PersonBean("Bar", 53)));

    PCollection<OrderBean> orders =
        PBegin.in(pipeline)
            .apply(
                "orders",
                Create.of(
                    new OrderBean("Foo", 15),
                    new OrderBean("Foo", 10),
                    new OrderBean("Foo", 5),
                    new OrderBean("Bar", 53),
                    new OrderBean("Bar", 54),
                    new OrderBean("Bar", 55)));

    String sql =
        "SELECT name, amount "
            + "FROM buyers INNER JOIN orders "
            + "ON buyerName = name "
            + "WHERE ageYears = 5";

    PCollection<Row> result =
        tuple(
                "buyers", people,
                "orders", orders)
            .apply("sql", SqlTransform.query(sql));

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.rowsBuilderOf(
                    Schema.builder().addStringField("name").addInt32Field("amount").build())
                .addRows(
                    "Foo", 15,
                    "Foo", 10,
                    "Foo", 5)
                .getRows());

    pipeline.run();
  }

  @Test
  public void testAggregation() {
    PCollection<PersonBean> people =
        PBegin.in(pipeline)
            .apply("people", Create.of(new PersonBean("Foo", 5), new PersonBean("Bar", 53)));

    PCollection<OrderBean> orders =
        PBegin.in(pipeline)
            .apply(
                "orders",
                Create.of(
                    new OrderBean("Foo", 15),
                    new OrderBean("Foo", 10),
                    new OrderBean("Foo", 5),
                    new OrderBean("Bar", 53),
                    new OrderBean("Bar", 54),
                    new OrderBean("Bar", 55)));

    String sql =
        "SELECT name, SUM(amount) as total "
            + "FROM buyers INNER JOIN orders "
            + "ON buyerName = name "
            + "GROUP BY name";

    PCollection<Row> result =
        tuple(
                "buyers", people,
                "orders", orders)
            .apply("sql", SqlTransform.query(sql));

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.rowsBuilderOf(
                    Schema.builder().addStringField("name").addInt32Field("total").build())
                .addRows(
                    "Foo", 30,
                    "Bar", 162)
                .getRows());

    pipeline.run();
  }
}
