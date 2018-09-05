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

import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.values.TupleTag;

/** Right outer join implementation of {@link JoinFn}. */
public class RightOuterJoinFn<LeftT, RightT, K, OutputT> extends JoinFn<LeftT, RightT, K, OutputT> {

  public RightOuterJoinFn(
      BinaryFunctor<LeftT, RightT, OutputT> joiner,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag,
      String operatorName,
      AccumulatorProvider accumulatorProvider) {
    super(joiner, leftTag, rightTag, operatorName, accumulatorProvider);
  }

  @Override
  protected void doJoin(Iterable<LeftT> leftSideIter, Iterable<RightT> rightSideIter) {

    for (RightT rightValue : rightSideIter) {
      if (leftSideIter.iterator().hasNext()) {
        for (LeftT leftValue : leftSideIter) {
          joiner.apply(leftValue, rightValue, resultsCollector);
        }
      } else {
        joiner.apply(null, rightValue, resultsCollector);
      }
    }
  }

  @Override
  public String getFnName() {
    return "::right-outer-join";
  }
}
