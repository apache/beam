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
package org.apache.beam.sdk.extensions.ml;

import static org.junit.Assert.assertEquals;

import com.google.privacy.dlp.v2.Table;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BatchRequestForDlpTest {

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void batchesRequests() {
    PCollection<KV<String, Iterable<Table.Row>>> batchedRows =
        testPipeline
            .apply(Create.of(KV.of("key", "value1"), KV.of("key", "value2")))
            .apply(ParDo.of(new MapStringToDlpRow(null)))
            .apply(ParDo.of(new BatchRequestForDLP(524000)));
    PAssert.that(batchedRows).satisfies(new VerifyPCollectionSize());
    testPipeline.run().waitUntilFinish();
  }

  private static class VerifyPCollectionSize
      implements SerializableFunction<Iterable<KV<String, Iterable<Table.Row>>>, Void> {
    @Override
    public Void apply(Iterable<KV<String, Iterable<Table.Row>>> input) {
      List<KV<String, Iterable<Table.Row>>> itemList = new ArrayList<>();
      input.forEach(itemList::add);
      assertEquals(1, itemList.size());
      return null;
    }
  }
}
