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
import { Impulse } from "./internal";
import { Root, PCollection } from "../pvalue";

/**
 * A Ptransform that represents a 'static' source with a list of elements passed at construction time. It
 * returns a PCollection that contains the elements in the input list.
 *
 * @extends PTransform
 */
export class Create<T> extends PTransform<Root, PCollection<T>> {
  elements: T[];

  /**
   * Construct a new Create PTransform.
   * @param elements - the list of elements in the PCollection
   */
  constructor(elements: T[]) {
    super("Create");
    this.elements = elements;
  }

  expand(root: Root) {
    const elements = this.elements;
    // TODO: (Cleanup) Store encoded values and conditionally shuffle.
    return root.apply(new Impulse()).flatMap(
      withName("ExtractElements", function* blarg(_) {
        yield* elements;
      })
    );
  }
}
