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

import { PTransform, withName } from "./transform";
import { impulse } from "./internal";
import * as utils from "./utils";
import { Root, PCollection } from "../pvalue";

/**
 * A Ptransform that represents a 'static' source with a list of elements passed at construction time. It
 * returns a PCollection that contains the elements in the input list.
 */
export function create<T>(
  elements: T[],
  reshuffle: boolean = true,
): PTransform<Root, PCollection<T>> {
  function create(root: Root): PCollection<T> {
    const pcoll = root
      .apply(impulse())
      .flatMap(withName("ExtractElements", (_) => elements));
    if (elements.length > 1 && reshuffle) {
      return pcoll.apply(utils.reshuffle());
    } else {
      return pcoll;
    }
  }

  return create;
}
