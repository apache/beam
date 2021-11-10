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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Objects;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRelError;
import org.apache.beam.sdk.extensions.sql.impl.udf.CustomUdafWithError;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class BeamSqlErrorTest extends BeamSqlDslBase {

  @Test
  public void testFailedExpression() {
    Schema resultType = Schema.builder().addByteArrayField("field").build();

    String sql = "SELECT CustomUdafWithError(f_bytes) FROM PCOLLECTION WHERE f_func = 'HashingFn'";

    PTransform mock = spy(PTransform.class);
    when(mock.expand(any()))
        .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0, PCollection.class));

    ArgumentCaptor<PCollection> captor = ArgumentCaptor.forClass(PCollection.class);

    PCollection<Row> result =
        boundedInputBytes
            .apply(
                "testUdf",
                SqlTransform.query(sql)
                    .withAutoLoading(false)
                    .withErrorsTransformer(mock)
                    .registerUdf("CustomUdafWithError", CustomUdafWithError.class))
            .setCoder(SchemaCoder.of(resultType));

    PAssert.that(result).empty();
    Object[] expectedElements =
        rowsOfBytes.stream()
            .filter(row -> Objects.equals(row.getValue("f_func"), "HashingFn"))
            .map(row -> BeamCalcRelError.create(row, "test error"))
            .toArray();
    Mockito.verify(mock, times(1)).expand(captor.capture());
    PAssert.that(captor.getValue()).containsInAnyOrder(expectedElements);
    pipeline.run().waitUntilFinish();
  }
}
