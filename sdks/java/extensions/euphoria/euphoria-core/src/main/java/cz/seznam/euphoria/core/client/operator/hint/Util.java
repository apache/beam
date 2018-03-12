/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.operator.hint;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.Operator;

import java.util.ArrayList;

public class Util {

  @SuppressWarnings("unchecked")
  public static boolean wantTranslateBroadcastHashJoin(Join o) {
    final ArrayList<Dataset> inputs = new ArrayList(o.listInputs());
    if (inputs.size() != 2) {
      return false;
    }
    final Dataset leftDataset = inputs.get(0);
    final Dataset rightDataset = inputs.get(1);
    return
        (o.getType() == Join.Type.LEFT && hasFitsInMemoryHint(rightDataset.getProducer()) ||
            o.getType() == Join.Type.RIGHT && hasFitsInMemoryHint(leftDataset.getProducer())
        ) && !(o.getWindowing() instanceof MergingWindowing);
  }

  public static boolean hasFitsInMemoryHint(Operator operator) {
    return operator != null &&
        operator.getHints() != null &&
        operator.getHints().contains(SizeHint.FITS_IN_MEMORY);
  }
}
