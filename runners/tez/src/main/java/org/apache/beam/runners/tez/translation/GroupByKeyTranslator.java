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

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GroupByKey} translation to Tez {@link org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig}
 */
class GroupByKeyTranslator<K, V> implements TransformTranslator<GroupByKey<K, V>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKey.class);

  @Override
  public void translate(GroupByKey<K, V> transform, TranslationContext context) {
    if (context.getCurrentInputs().size() > 1 ){
      throw new RuntimeException("Multiple Inputs are not yet supported");
    } else if (context.getCurrentOutputs().size() > 1){
      throw new RuntimeException("Multiple Outputs are not yet supported");
    }
    PValue input = Iterables.getOnlyElement(context.getCurrentInputs().values());
    PValue output = Iterables.getOnlyElement(context.getCurrentOutputs().values());
    context.addShufflePair(input, output);
  }
}