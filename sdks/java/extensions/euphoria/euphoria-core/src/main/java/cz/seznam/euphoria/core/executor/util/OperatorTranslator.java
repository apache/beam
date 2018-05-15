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
package cz.seznam.euphoria.core.executor.util;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.Join.Type;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.hint.SizeHint;
import java.util.ArrayList;

/** Util class when specific executors use the same methods for operator translation. */
@Audience(Audience.Type.EXECUTOR)
public class OperatorTranslator {

  @SuppressWarnings("unchecked")
  public static boolean wantTranslateBroadcastHashJoin(Join o) {
    final ArrayList<Dataset> inputs = new ArrayList(o.listInputs());
    if (inputs.size() != 2) {
      return false;
    }
    final Dataset leftDataset = inputs.get(0);
    final Dataset rightDataset = inputs.get(1);
    return (o.getType() == Join.Type.LEFT && hasFitsInMemoryHint(rightDataset.getProducer())
            || o.getType() == Type.RIGHT && hasFitsInMemoryHint(leftDataset.getProducer()))
        && !(o.getWindowing() instanceof MergingWindowing);
  }

  public static boolean hasFitsInMemoryHint(Operator operator) {
    return operator != null
        && operator.getHints() != null
        && operator.getHints().contains(SizeHint.FITS_IN_MEMORY);
  }
}
