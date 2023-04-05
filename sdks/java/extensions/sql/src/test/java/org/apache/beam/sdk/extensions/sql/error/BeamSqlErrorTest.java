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
package org.apache.beam.sdk.extensions.sql.error;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class BeamSqlErrorTest {
  private static final String ID = "id";
  private static final String AMOUNT = "amount";
  private static final String COUNTRY_CODE = "country_code";
  private static final String CURRENCY = "currency";
  private static final String invalidAmount = "100$";
  private static final String invalidCurrency = "*";
  private static final String SUM_AMOUNT = "sum_amount";
  private static final String F_3 = "f3";
  private static final String ROW = "row";
  private static final String ERROR = "error";
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  protected PCollection<Row> boundedInputBytes;
  static List<Row> inputRows;
  static Schema inputType;

  @BeforeClass
  public static void prepareClass() {
    inputType =
        Schema.builder()
            .addStringField(ID)
            .addStringField(AMOUNT)
            .addStringField(COUNTRY_CODE)
            .addStringField(CURRENCY)
            .build();

    inputRows =
        TestUtils.RowsBuilder.of(inputType)
            .addRows(
                "1",
                "100",
                "US",
                "$",
                "2",
                invalidAmount,
                "US",
                "$",
                "3",
                "100",
                "US",
                invalidCurrency)
            .getRows();
  }

  @Before
  public void preparePCollections() {
    boundedInputBytes =
        pipeline.apply("boundedInput", Create.of(inputRows).withRowSchema(inputType));
  }

  @Test
  public void testFailedExpression() {
    Schema resultType =
        Schema.builder()
            .addStringField(ID)
            .addStringField(COUNTRY_CODE)
            .addDoubleField(SUM_AMOUNT)
            .build();
    Schema midResultType =
        Schema.builder()
            .addStringField(ID)
            .addStringField(COUNTRY_CODE)
            .addStringField(CURRENCY)
            .addInt64Field(F_3)
            .build();

    String sql =
        "SELECT id,country_code,CalculatePrice(sum(CastUdf(amount)),currency) as sum_amount FROM PCOLLECTION group by id,country_code,currency";

    PTransform mock = spy(PTransform.class);
    when(mock.expand(Matchers.any()))
        .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0, PCollection.class));

    ArgumentCaptor<PCollection> captor = ArgumentCaptor.forClass(PCollection.class);

    PCollection<Row> validRowsResult =
        boundedInputBytes
            .apply(
                "calculate",
                SqlTransform.query(sql)
                    .withAutoLoading(false)
                    .withErrorsTransformer(mock)
                    .registerUdf("CastUdf", CastUdf.class)
                    .registerUdf("CalculatePrice", CalculatePrice.class))
            .setCoder(SchemaCoder.of(resultType));

    PAssert.that(validRowsResult)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(resultType).addRows("1", "US", 100.0).getRows());

    Schema firstErrorSchema =
        Schema.builder().addRowField(ROW, inputType).addStringField(ERROR).build();

    Row failedOnFirstUdfElement =
        TestTableUtils.buildRows(
                firstErrorSchema,
                Arrays.asList(
                    TestTableUtils.buildRows(
                            inputType, Arrays.asList("2", invalidAmount, "US", "$"))
                        .get(0),
                    "Found invalid value " + invalidAmount))
            .get(0);

    Schema secondErrorSchema =
        Schema.builder().addRowField(ROW, midResultType).addStringField(ERROR).build();

    Row failedOnSecondUdfElement =
        TestTableUtils.buildRows(
                secondErrorSchema,
                Arrays.asList(
                    TestTableUtils.buildRows(
                            midResultType, Arrays.asList("3", "US", invalidCurrency, 100L))
                        .get(0),
                    "Currency isn't supported " + invalidCurrency))
            .get(0);

    Mockito.verify(mock, times(2)).expand(captor.capture());
    PAssert.that(captor.getAllValues().get(0)).containsInAnyOrder(failedOnFirstUdfElement);
    PAssert.that(captor.getAllValues().get(1)).containsInAnyOrder(failedOnSecondUdfElement);
    pipeline.run().waitUntilFinish();
  }
}
