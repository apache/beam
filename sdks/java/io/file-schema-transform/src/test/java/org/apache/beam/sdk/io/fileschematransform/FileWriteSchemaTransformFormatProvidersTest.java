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
package org.apache.beam.sdk.io.fileschematransform;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileWriteSchemaTransformFormatProviders}. */
@RunWith(JUnit4.class)
public class FileWriteSchemaTransformFormatProvidersTest {

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void loadProviders() {
    Map<String, FileWriteSchemaTransformFormatProvider> formatProviderMap =
        FileWriteSchemaTransformFormatProviders.loadProviders();
    Set<String> keys = formatProviderMap.keySet();
    assertTrue(keys.contains("avro"));
    assertTrue(keys.contains("csv"));
    assertTrue(keys.contains("json"));
    assertTrue(keys.contains("parquet"));
    assertTrue(keys.contains("xml"));
  }

  @Test
  public void testAvro() {}

  @Test
  public void testCsv() {}

  @Test
  public void testJson() {
    // FileWriteSchemaTransformFormatProvider provider =
    // FileWriteSchemaTransformFormatProviders.loadProviders().get(JSON);
    // provider.buildTransform(FileWriteSchemaTransformConfiguration.builder().build(),
    //     SchemaAwareJavaBeans.SIMPLE_BEAN_SCHEMA);
  }

  @Test
  public void testParquet() {}

  @Test
  public void testXml() {}

  @Test
  public void buildRowToRecordFn() {}

  @Test
  public void buildFileIOWrite() {}

  @Test
  public void buildTextIOWrite() {}
}
