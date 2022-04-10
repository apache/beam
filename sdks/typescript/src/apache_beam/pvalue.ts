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

import { Pipeline } from "./internal/pipeline";

import {
  PTransform,
  AsyncPTransform,
  extractName,
  withName,
} from "./transforms/transform";
import { ParDo, DoFn } from "./transforms/pardo";
import * as runnerApi from "./proto/beam_runner_api";

/**
 * The base object on which one can start building a Beam DAG.
 * Generally followed by a source-like transform such as a read or impulse.
 */
export class Root {
  pipeline: Pipeline;

  constructor(pipeline: Pipeline) {
    this.pipeline = pipeline;
  }

  apply<OutputT extends PValue<any>>(
    transform: PTransform<Root, OutputT> | ((Root) => OutputT)
  ) {
    if (!(transform instanceof PTransform)) {
      transform = new PTransformFromCallable(transform);
    }
    return this.pipeline.applyTransform(transform, this);
  }

  async asyncApply<OutputT extends PValue<any>>(
    transform: AsyncPTransform<Root, OutputT> | ((Root) => Promise<OutputT>)
  ) {
    if (!(transform instanceof AsyncPTransform)) {
      transform = new AsyncPTransformFromCallable(transform);
    }
    return await this.pipeline.asyncApplyTransform(transform, this);
  }
}

/**
 * A deferred, possibly distributed collection of elements.
 */
export class PCollection<T> {
  type: string = "pcollection";
  pipeline: Pipeline;
  private id: string;
  private computeId: () => string;

  constructor(pipeline: Pipeline, id: string | (() => string)) {
    this.pipeline = pipeline;
    if (typeof id == "string") {
      this.id = id;
    } else {
      this.computeId = id;
    }
  }

  getId(): string {
    if (this.id == undefined) {
      this.id = this.computeId();
    }
    return this.id;
  }

  apply<OutputT extends PValue<any>>(
    transform: PTransform<PCollection<T>, OutputT> | ((PCollection) => OutputT)
  ) {
    if (!(transform instanceof PTransform)) {
      transform = new PTransformFromCallable(transform);
    }
    return this.pipeline.applyTransform(transform, this);
  }

  asyncApply<OutputT extends PValue<any>>(
    transform:
      | AsyncPTransform<PCollection<T>, OutputT>
      | ((PCollection) => Promise<OutputT>)
  ) {
    if (!(transform instanceof AsyncPTransform)) {
      transform = new AsyncPTransformFromCallable(transform);
    }
    return this.pipeline.asyncApplyTransform(transform, this);
  }

  map<OutputT, ContextT>(
    fn:
      | (ContextT extends undefined ? (element: T) => OutputT : never)
      | ((element: T, context: ContextT) => OutputT),
    context: ContextT = undefined!
  ): PCollection<OutputT> {
    return this.apply(
      withName(
        "map(" + extractName(fn) + ")",
        new ParDo<T, OutputT, ContextT>(
          {
            process: function* (element: T, context: ContextT) {
              // While it's legal to call a function with extra arguments which will
              // be ignored, this can have surprising behavior (e.g. for map(console.log))
              yield context == undefined
                ? (fn as (T) => OutputT)(element)
                : fn(element, context);
            },
          },
          context
        )
      )
    );
  }

  flatMap<OutputT, ContextT>(
    fn:
      | (ContextT extends undefined ? (element: T) => Iterable<OutputT> : never)
      | ((element: T, context: ContextT) => Iterable<OutputT>),
    context: ContextT = undefined!
  ): PCollection<OutputT> {
    return this.apply(
      withName(
        "flatMap(" + extractName(fn) + ")",
        new ParDo<T, OutputT, ContextT>(
          {
            process: function (element: T, context: ContextT) {
              // While it's legal to call a function with extra arguments which will
              // be ignored, this can have surprising behavior (e.g. for map(console.log))
              return context == undefined
                ? (fn as (T) => Iterable<OutputT>)(element)
                : fn(element, context);
            },
          },
          context
        )
      )
    );
  }

  root(): Root {
    return new Root(this.pipeline);
  }
}

/**
 * The type of object that may be consumed or produced by a PTransform.
 */
export type PValue<T> =
  | void
  | Root
  | PCollection<T>
  | PValue<T>[]
  | { [key: string]: PValue<T> };

/**
 * Returns a PValue as a flat object with string keys and PCollection values.
 *
 * The full set of PCollections reachable by this PValue will be returned,
 * with keys corresponding roughly to the path taken to get there.
 */
export function flattenPValue<T>(
  pValue: PValue<T>,
  prefix: string = ""
): { [key: string]: PCollection<T> } {
  const result: { [key: string]: PCollection<any> } = {};
  if (pValue == null) {
    // pass
  } else if (pValue instanceof Root) {
    // pass
  } else if (pValue instanceof PCollection) {
    if (prefix) {
      result[prefix] = pValue;
    } else {
      result.main = pValue;
    }
  } else {
    if (prefix) {
      prefix += ".";
    }
    if (pValue instanceof Array) {
      for (var i = 0; i < pValue.length; i++) {
        Object.assign(result, flattenPValue(pValue[i], prefix + i));
      }
    } else {
      for (const [key, subValue] of Object.entries(pValue)) {
        Object.assign(result, flattenPValue(subValue, prefix + key));
      }
    }
  }
  return result;
}

/**
 * Wraps a PValue in a single object such that a transform can be applied to it.
 *
 * For example, Flatten takes a PCollection[] as input, but Array has no
 * apply(PTransform) method, so one writes
 *
 *    P([pcA, pcB, pcC]).apply(new Flatten())
 */
export function P<T extends PValue<any>>(pvalue: T) {
  return new PValueWrapper(pvalue);
}

class PValueWrapper<T extends PValue<any>> {
  constructor(private pvalue: T) {}

  apply<O extends PValue<any>>(
    transform: PTransform<T, O> | ((input: T) => O),
    root: Root | null = null
  ) {
    if (!(transform instanceof PTransform)) {
      transform = new PTransformFromCallable(transform);
    }
    return this.pipeline(root).applyTransform(transform, this.pvalue);
  }

  async asyncApply<O extends PValue<any>>(
    transform: AsyncPTransform<T, O> | ((input: T) => Promise<O>),
    root: Root | null = null
  ) {
    if (!(transform instanceof AsyncPTransform)) {
      transform = new AsyncPTransformFromCallable(transform);
    }
    return await this.pipeline(root).asyncApplyTransform(
      transform,
      this.pvalue
    );
  }

  private pipeline(root: Root | null = null) {
    if (root == null) {
      const flat = flattenPValue(this.pvalue);
      return Object.values(flat)[0].pipeline;
    } else {
      return root.pipeline;
    }
  }
}

class PTransformFromCallable<
  InputT extends PValue<any>,
  OutputT extends PValue<any>
> extends PTransform<InputT, OutputT> {
  expander: (InputT) => OutputT;

  constructor(expander: (InputT) => OutputT) {
    super(extractName(expander));
    this.expander = expander;
  }

  expand(input: InputT) {
    return this.expander(input);
  }
}

class AsyncPTransformFromCallable<
  InputT extends PValue<any>,
  OutputT extends PValue<any>
> extends AsyncPTransform<InputT, OutputT> {
  expander: (InputT) => Promise<OutputT>;

  constructor(expander: (InputT) => Promise<OutputT>) {
    super(extractName(expander));
    this.expander = expander;
  }

  async asyncExpand(input: InputT) {
    return this.expander(input);
  }
}

import { requireForSerialization } from "./serialization";
requireForSerialization("apache_beam.pvalue", exports);
