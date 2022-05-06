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

import * as external from "./external";
import * as internal from "./internal";
import * as transform from "./transform";
import * as row_coder from "../coders/row_coder";
import { P, PCollection } from "../pvalue";
import { serviceProviderFromJavaGradleTarget } from "../utils/service";

/**
 * Runs an SQL statement over a set of input PCollection(s).
 *
 * The input can either be a single PCollection, in which case the table is
 * named PCOLLECTION, or an object with PCollection values, in which case the
 * corresponding names can be used in the sql statement.
 *
 * The input(s) must be schema'd (i.e. use the RowCoder). This can be done
 * by explicitly setting the schema with internal.WithCoderInternal or passing
 * a prototype element in as a second argument, e.g.
 *
 * pcoll.applyAsync(
 *    new SqlTransform(
 *        "select a, b from PCOLLECTION",
 *        {a: 0, b: "string"},
 *    ));
 */
export class SqlTransform<
  InputT extends PCollection<any> | { [key: string]: PCollection<any> }
> extends transform.AsyncPTransform<InputT, PCollection<any>> {
  // TOOD: (API) (Typescript): How to infer input_types, or at least make it optional.
  constructor(private query: string, private inputTypes = null) {
    // TODO: Unique names. Should we truncate/omit the full SQL statement?
    super("Sql(" + query + ")");
  }

  async asyncExpand(input: InputT): Promise<PCollection<any>> {
    function withCoder<T>(pcoll: PCollection<T>, type): PCollection<T> {
      if (type == null) {
        if (
          !(
            pcoll.pipeline.context.getPCollectionCoder(pcoll) instanceof
            row_coder.RowCoder
          )
        ) {
          throw new Error(
            "SqlTransform can only be applied to schema'd transforms. " +
              "Please ensure the input PCollection(s) have a RowCoder, " +
              "or pass a prototypical element in as the second argument " +
              "of SqlTransform so that one can be inferred."
          );
        }
        return pcoll;
      }
      return pcoll.apply(
        new internal.WithCoderInternal(row_coder.RowCoder.fromJSON(type))
      );
    }

    if (input instanceof PCollection) {
      input = withCoder(input, this.inputTypes) as InputT;
    } else {
      input = Object.fromEntries(
        Object.keys(input).map((tag) => [
          tag,
          withCoder(
            input[tag],
            this.inputTypes == null ? null : this.inputTypes[tag]
          ),
        ])
      ) as InputT;
    }

    return await P(input).asyncApply(
      new external.RawExternalTransform(
        "beam:external:java:sql:v1",
        { query: this.query },
        serviceProviderFromJavaGradleTarget(
          "sdks:java:extensions:sql:expansion-service:shadowJar"
        )
      )
    );
  }
}
