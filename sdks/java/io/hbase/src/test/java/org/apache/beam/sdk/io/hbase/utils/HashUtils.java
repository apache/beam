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
package org.apache.beam.sdk.io.hbase.utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

/**
 * <b>Internal only:</b> Utility functions to help assert equality between mutation lists for
 * testing purposes.
 */
public class HashUtils {

  /**
   * Asserts two {@link RowMutations} objects are equal by rowkey and list of {@link Mutation}.
   *
   * @param rowMutationA
   * @param rowMutationB
   * @throws Exception if hash function fails
   */
  public static void assertRowMutationsEquals(RowMutations rowMutationA, RowMutations rowMutationB)
      throws Exception {
    if (rowMutationA == null || rowMutationB == null) {
      Assert.assertEquals(rowMutationA, rowMutationB);
    }
    Assert.assertTrue(Bytes.equals(rowMutationA.getRow(), rowMutationB.getRow()));
    Assert.assertEquals(
        hashMutationList(rowMutationA.getMutations()),
        hashMutationList(rowMutationB.getMutations()));
  }

  /**
   * Hashes list of {@link Mutation} into String, by iterating through Mutation {@link Cell} and
   * picking out relevant attributes for comparison.
   *
   * <p>Different mutation types may have different hashing treatments.
   *
   * @param mutationList
   * @return list of mutation strings that can be compared to other hashed mutation lists.
   */
  public static List<String> hashMutationList(List<Mutation> mutationList) throws Exception {
    List<String> mutations = new ArrayList<>();
    for (Mutation mutation : mutationList) {
      List<String> cells = new ArrayList<>();

      CellScanner scanner = mutation.cellScanner();
      while (scanner.advance()) {
        Cell c = scanner.current();
        String mutationType = "";
        long ts = 0;

        if (KeyValue.Type.codeToType(c.getTypeByte()).equals(KeyValue.Type.DeleteFamily)) {
          // DeleteFamily has its timestamp created at runtime and cannot be compared with accuracy
          // during tests, so we remove the timestamp altogether.
          mutationType = "DELETE_FAMILY";
          ts = 0L;
        } else if (KeyValue.Type.codeToType(c.getTypeByte()).equals(KeyValue.Type.DeleteColumn)) {
          mutationType = "DELETE_COLUMN";
          ts = c.getTimestamp();
        } else if (KeyValue.Type.codeToType(c.getTypeByte()).equals(KeyValue.Type.Put)) {
          mutationType = "PUT";
          ts = c.getTimestamp();
        } else {
          throw new Exception("hashMutationList error: Cell type is not supported.");
        }

        String cellHash =
            String.join(
                "_",
                mutationType,
                Long.toString(ts),
                Bytes.toString(CellUtil.cloneRow(c)),
                Bytes.toString(CellUtil.cloneFamily(c)),
                Bytes.toString(CellUtil.cloneQualifier(c)),
                Bytes.toString(CellUtil.cloneValue(c)));

        cells.add(cellHash);
      }

      mutations.add(String.join(" > ", cells));
    }

    return mutations;
  }

  /**
   * {@link RowMutations} assert equality on rowkey only and does not guarantee that its mutations
   * are the same nor that they are in the same order.
   *
   * <p>This transform splits a RowMutations object into <rowkey String, List<MutationsToString> so
   * that two RowMutations objects can be compared via {@link org.apache.beam.sdk.testing.PAssert}.
   */
  public static class HashHbaseRowMutations
      extends PTransform<
          PCollection<KV<byte[], RowMutations>>, PCollection<KV<String, List<String>>>> {

    @Override
    public PCollection<KV<String, List<String>>> expand(
        PCollection<KV<byte[], RowMutations>> input) {
      return input.apply(ParDo.of(new HashHbaseRowMutationsFn()));
    }
  }

  static class HashHbaseRowMutationsFn
      extends DoFn<KV<byte[], RowMutations>, KV<String, List<String>>> {

    public HashHbaseRowMutationsFn() {}

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      RowMutations rowMutations = c.element().getValue();

      if (!Bytes.equals(c.element().getKey(), rowMutations.getRow())) {
        throw new Exception("Hash error, KV rowkey is not the same as rowMutations rowkey");
      }

      c.output(
          KV.of(
              Bytes.toString(rowMutations.getRow()),
              hashMutationList(rowMutations.getMutations())));
    }
  }
}
