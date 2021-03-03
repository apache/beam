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
package org.apache.beam.sdk.io.hcatalog;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Assert;
import org.junit.Test;

public class SchemaUtilsTest {
  @Test
  public void testParameterizedTypesToBeamTypes() {
    List<FieldSchema> listOfFieldSchema = new ArrayList<>();
    listOfFieldSchema.add(new FieldSchema("parameterizedChar", "char(10)", null));
    listOfFieldSchema.add(new FieldSchema("parameterizedVarchar", "varchar(100)", null));
    listOfFieldSchema.add(new FieldSchema("parameterizedDecimal", "decimal(30,16)", null));

    Schema expectedSchema =
        Schema.builder()
            .addNullableField("parameterizedChar", Schema.FieldType.STRING)
            .addNullableField("parameterizedVarchar", Schema.FieldType.STRING)
            .addNullableField("parameterizedDecimal", Schema.FieldType.DECIMAL)
            .build();

    Schema actualSchema = SchemaUtils.toBeamSchema(listOfFieldSchema);
    Assert.assertEquals(expectedSchema, actualSchema);
  }
}
