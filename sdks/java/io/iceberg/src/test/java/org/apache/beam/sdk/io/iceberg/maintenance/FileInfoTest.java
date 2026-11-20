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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileInfoTest {

  @Test
  public void testSchemaCoderRoundTrip() throws Exception {
    SchemaCoder<FileInfo> coder = SchemaRegistry.createDefault().getSchemaCoder(FileInfo.class);
    for (FileCategory category : FileCategory.values()) {
      for (boolean reachable : new boolean[] {true, false}) {
        FileInfo info =
            FileInfo.of("s3://bucket/table/data/a.parquet", category, reachable, "db.t");
        assertEquals(info, CoderUtils.clone(coder, info));
      }
    }
  }

  @Test
  public void testCategoryIsEncodedAsEnumeration() throws Exception {
    Schema schema = SchemaRegistry.createDefault().getSchema(FileInfo.class);
    Schema.FieldType categoryType = schema.getField("category").getType();
    assertTrue(categoryType.getLogicalType() instanceof EnumerationType);
  }

  @Test
  @SuppressWarnings("nullness") // Deliberately passes null to verify rejection.
  public void testRequiresTableIdentifier() {
    assertThrows(
        NullPointerException.class,
        () -> FileInfo.of("/tmp/a.parquet", FileCategory.DATA, false, null));
  }
}
