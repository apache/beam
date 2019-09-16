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
package org.apache.beam.runners.gearpump.translators;

import io.gearpump.streaming.dsl.javaapi.JavaStream;
import java.util.List;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;

/** CreateGearpumpPCollectionView bridges input stream to down stream transforms. */
public class CreateGearpumpPCollectionViewTranslator<ElemT, ViewT>
    implements TransformTranslator<
        CreateStreamingGearpumpView.CreateGearpumpPCollectionView<ElemT, ViewT>> {

  private static final long serialVersionUID = -3955521308055056034L;

  @Override
  public void translate(
      CreateStreamingGearpumpView.CreateGearpumpPCollectionView<ElemT, ViewT> transform,
      TranslationContext context) {
    JavaStream<WindowedValue<List<ElemT>>> inputStream = context.getInputStream(context.getInput());
    PCollectionView<ViewT> view = transform.getView();
    context.setOutputStream(view, inputStream);
  }
}
