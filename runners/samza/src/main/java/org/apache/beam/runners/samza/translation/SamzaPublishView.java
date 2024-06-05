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
package org.apache.beam.runners.samza.translation;

import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Samza {@link PTransform} that creates a primitive output {@link PCollection}, as the results of a
 * {@link PCollectionView}.
 */
class SamzaPublishView<ElemT, ViewT>
    extends PTransform<PCollection<List<ElemT>>, PCollection<List<ElemT>>> {
  static final String SAMZA_PUBLISH_VIEW_URN = "beam:transform:samza:publish-view:v1";

  private final PCollectionView<ViewT> view;

  SamzaPublishView(PCollectionView<ViewT> view) {
    this.view = view;
  }

  @Override
  public PCollection<List<ElemT>> expand(PCollection<List<ElemT>> input) {
    return PCollection.<List<ElemT>>createPrimitiveOutputInternal(
        input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), input.getCoder());
  }

  public PCollectionView<ViewT> getView() {
    return view;
  }

  @Override
  public String getName() {
    return view.getName();
  }

  static class SamzaPublishViewPayloadTranslator
      extends PTransformTranslation.TransformPayloadTranslator.NotSerializable<
          SamzaPublishView<?, ?>> {

    SamzaPublishViewPayloadTranslator() {}

    @Override
    public String getUrn() {
      return SAMZA_PUBLISH_VIEW_URN;
    }
  }
}
