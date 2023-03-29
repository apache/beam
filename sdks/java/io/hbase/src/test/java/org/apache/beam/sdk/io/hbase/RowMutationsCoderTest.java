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
package org.apache.beam.sdk.io.hbase;

import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colFamily;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colFamily2;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colQualifier;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colQualifier2;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.rowKey;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.timeT;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.io.hbase.utils.HashUtils;
import org.apache.beam.sdk.io.hbase.utils.TestHBaseUtils;
import org.apache.beam.sdk.io.hbase.utils.TestHBaseUtils.HBaseMutationBuilder;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test that {@link HBaseRowMutationsCoder} encoding does not change {@link RowMutations} object.
 */
@RunWith(JUnit4.class)
public class RowMutationsCoderTest {

  private final HBaseRowMutationsCoder coder = HBaseRowMutationsCoder.of();
  private ByteArrayOutputStream outputStream;
  private ByteArrayInputStream inputStream;

  @Before
  public void setUp() {
    outputStream = new ByteArrayOutputStream();
  }

  @After
  public void tearDown() throws IOException {
    outputStream.close();
  }

  @Test
  public void testEncodePut() throws Exception {
    RowMutations put = new RowMutations(rowKey);
    put.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey, colFamily, colQualifier, value, timeT));
    coder.encode(put, outputStream);

    inputStream = new ByteArrayInputStream(outputStream.toByteArray());

    RowMutations decodedPut = coder.decode(inputStream);

    Assert.assertTrue(inputStream.available() == 0);
    HashUtils.assertRowMutationsEquals(put, decodedPut);
  }

  @Test
  public void testEncodeMultipleMutations() throws Exception {
    RowMutations multipleMutations = new RowMutations(rowKey);
    multipleMutations.add(
        HBaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT));
    multipleMutations.add(
        TestHBaseUtils.HBaseMutationBuilder.createDelete(rowKey, colFamily, colQualifier, timeT));
    multipleMutations.add(
        TestHBaseUtils.HBaseMutationBuilder.createDeleteFamily(rowKey, colFamily, timeT));
    coder.encode(multipleMutations, outputStream);

    inputStream = new ByteArrayInputStream(outputStream.toByteArray());

    RowMutations decodedMultipleMutations = coder.decode(inputStream);

    Assert.assertTrue(inputStream.available() == 0);
    HashUtils.assertRowMutationsEquals(multipleMutations, decodedMultipleMutations);
  }

  @Test
  public void testEncodeMultipleRowMutations() throws Exception {
    RowMutations put = new RowMutations(rowKey);
    put.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey, colFamily, colQualifier, value, timeT));
    RowMutations deleteCols = new RowMutations(rowKey);
    deleteCols.add(
        TestHBaseUtils.HBaseMutationBuilder.createDelete(rowKey, colFamily, colQualifier, timeT));
    RowMutations deleteFamily = new RowMutations(rowKey);
    deleteFamily.add(
        TestHBaseUtils.HBaseMutationBuilder.createDeleteFamily(rowKey, colFamily, timeT));

    coder.encode(put, outputStream);
    coder.encode(deleteCols, outputStream);
    coder.encode(deleteFamily, outputStream);

    inputStream = new ByteArrayInputStream(outputStream.toByteArray());

    RowMutations decodedPut = coder.decode(inputStream);
    RowMutations decodedDeleteCols = coder.decode(inputStream);
    RowMutations decodedDeleteFamily = coder.decode(inputStream);

    Assert.assertTrue(inputStream.available() == 0);
    HashUtils.assertRowMutationsEquals(put, decodedPut);
    HashUtils.assertRowMutationsEquals(deleteCols, decodedDeleteCols);
    HashUtils.assertRowMutationsEquals(deleteFamily, decodedDeleteFamily);
  }

  @Test
  public void testEncodeMultipleComplexRowMutations() throws Exception {
    RowMutations complexMutation = new RowMutations(rowKey);
    complexMutation.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey, colFamily, colQualifier, value, timeT));
    complexMutation.add(
        TestHBaseUtils.HBaseMutationBuilder.createDelete(
            rowKey, colFamily2, colQualifier2, timeT + 1));
    complexMutation.add(
        TestHBaseUtils.HBaseMutationBuilder.createDeleteFamily(rowKey, colFamily, timeT));

    coder.encode(complexMutation, outputStream);
    coder.encode(complexMutation, outputStream);
    coder.encode(complexMutation, outputStream);

    inputStream = new ByteArrayInputStream(outputStream.toByteArray());

    RowMutations decodedComplexMutation = coder.decode(inputStream);
    RowMutations decodedComplexMutation2 = coder.decode(inputStream);
    RowMutations decodedComplexMutation3 = coder.decode(inputStream);

    Assert.assertTrue(inputStream.available() == 0);
    HashUtils.assertRowMutationsEquals(complexMutation, decodedComplexMutation);
    HashUtils.assertRowMutationsEquals(complexMutation, decodedComplexMutation2);
    HashUtils.assertRowMutationsEquals(complexMutation, decodedComplexMutation3);
  }
}
