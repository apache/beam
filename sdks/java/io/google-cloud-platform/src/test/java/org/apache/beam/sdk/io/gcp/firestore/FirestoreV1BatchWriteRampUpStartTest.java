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
package org.apache.beam.sdk.io.gcp.firestore;

import static org.junit.Assert.assertEquals;

import com.google.firestore.v1.Write;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class FirestoreV1BatchWriteRampUpStartTest {

  @Test
  public void batchWrite_usesPipelineWideRampUpStart() {
    assertEquals(1, batchWriteSideInputs(PipelineOptionsFactory.create(), false).size());
  }

  @Test
  public void batchWriteWithDeadLetterQueue_usesPipelineWideRampUpStart() {
    assertEquals(1, batchWriteSideInputs(PipelineOptionsFactory.create(), true).size());
  }

  @Test
  public void batchWrite_olderUpdateCompatibilityVersion_keepsPreviousPipelineShape() {
    StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
    options.setUpdateCompatibilityVersion("2.77.0");
    assertEquals(0, batchWriteSideInputs(options, false).size());
  }

  private static List<PCollectionView<?>> batchWriteSideInputs(
      PipelineOptions options, boolean withDeadLetterQueue) {
    Pipeline pipeline = Pipeline.create(options);
    PCollection<Write> writes = pipeline.apply(Create.empty(ProtoCoder.of(Write.class)));
    if (withDeadLetterQueue) {
      writes.apply(FirestoreIO.v1().write().batchWrite().withDeadLetterQueue().build());
    } else {
      writes.apply(FirestoreIO.v1().write().batchWrite().build());
    }

    List<PCollectionView<?>> sideInputs = new ArrayList<>();
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            PTransform<?, ?> transform = node.getTransform();
            if (transform instanceof ParDo.SingleOutput
                && node.getFullName().endsWith("/batchWrite")) {
              sideInputs.addAll(((ParDo.SingleOutput<?, ?>) transform).getSideInputs().values());
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    return sideInputs;
  }
}
