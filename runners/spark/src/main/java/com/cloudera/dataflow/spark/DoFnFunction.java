/**
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
package com.cloudera.dataflow.spark;

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.streaming.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.List;

public class DoFnFunction<I, O> implements FlatMapFunction<Iterator<I>, O> {

  private final DoFn<I, O> fn;

  public DoFnFunction(DoFn<I, O> fn) {
    this.fn = fn;
  }

  @Override
  public Iterable<O> call(Iterator<I> iter) throws Exception {
    ProcCtxt<I, O> ctxt = new ProcCtxt(fn);
    fn.startBatch(ctxt);
    while (iter.hasNext()) {
      ctxt.element = iter.next();
      fn.processElement(ctxt);
    }
    fn.finishBatch(ctxt);
    return ctxt.outputs;
  }

  private static class ProcCtxt<I, O> extends DoFn<I, O>.ProcessContext {

    private List<O> outputs = Lists.newArrayList();
    private I element;

    public ProcCtxt(DoFn<I, O> fn) {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }

    @Override
    public void output(O o) {
      outputs.add(o);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tupleTag, T t) {
    }

    @Override
    public <T> T sideInput(TupleTag<T> tupleTag) {
      return null;
    }

    @Override
    public I element() {
      return element;
    }

    @Override
    public KeyedState keyedState() {
      return null;
    }
  }
}
