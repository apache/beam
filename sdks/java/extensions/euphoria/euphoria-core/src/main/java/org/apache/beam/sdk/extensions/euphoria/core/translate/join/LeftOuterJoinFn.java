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
package org.apache.beam.sdk.extensions.euphoria.core.translate.join;

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.translate.SingleValueCollector;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.TupleTag;

/** Left outer join implementation of {@link JoinFn}. */
public class LeftOuterJoinFn<LeftT, RightT, K, OutputT> extends JoinFn<LeftT, RightT, K, OutputT> {

  public LeftOuterJoinFn(
      BinaryFunctor<LeftT, RightT, OutputT> joiner,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {
    super(joiner, leftTag, rightTag);
  }

  @Override
  protected void doJoin(
      ProcessContext c,
      K key,
      CoGbkResult value,
      Iterable<LeftT> leftSideIter,
      Iterable<RightT> rightSideIter) {

    SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    for (LeftT leftValue : leftSideIter) {
      if (rightSideIter.iterator().hasNext()) {
        for (RightT rightValue : rightSideIter) {
          joiner.apply(leftValue, rightValue, outCollector);
          c.output(Pair.of(key, outCollector.get()));
        }
      } else {
        joiner.apply(leftValue, null, outCollector);
        c.output(Pair.of(key, outCollector.get()));
      }
    }
  }

  @Override
  public String getFnName() {
    return "::left-outer-join";
  }
}
