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
package org.apache.beam.runners.tez.translation;

import org.apache.beam.sdk.transforms.Flatten;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Flatten} translation to Tez equivalent.
 */
class FlattenPCollectionTranslator<T> implements TransformTranslator<Flatten.PCollections<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlattenPCollectionTranslator.class);

  @Override
  public void translate(Flatten.PCollections<T> transform, TranslationContext context) {
    //TODO: Translate transform to Tez and add to TranslationContext
  }
}