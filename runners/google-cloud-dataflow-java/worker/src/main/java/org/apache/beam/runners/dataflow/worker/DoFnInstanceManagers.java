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
package org.apache.beam.runners.dataflow.worker;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;

/** Common {@link DoFnInstanceManager} implementations. */
public class DoFnInstanceManagers {
  /**
   * Returns a {@link DoFnInstanceManager} that returns {@link DoFnInfo} instances obtained by
   * deserializing the provided bytes. {@link DoFnInstanceManager} will call {@link DoFn.Setup} as
   * required before returning the {@link DoFnInfo}, and {@link DoFn.Teardown} as appropriate.
   */
  public static DoFnInstanceManager cloningPool(DoFnInfo<?, ?> info) {
    return new ConcurrentQueueInstanceManager(info);
  }

  /**
   * Returns a {@link DoFnInstanceManager} that always returns the provided {@link DoFnInfo} from
   * calls to {@link DoFnInstanceManager#get()}.
   *
   * <p>Note that the returned {@link DoFnInstanceManager} will not call {@link DoFn.Setup} or
   * {@link DoFn.Teardown} under any circumstances, and will reuse the provided instance across all
   * threads, even if {@link DoFnInstanceManager#abort(DoFnInfo)} is called.
   */
  public static DoFnInstanceManager singleInstance(DoFnInfo<?, ?> info) {
    return new SingleInstanceManager(info);
  }

  private static class ConcurrentQueueInstanceManager implements DoFnInstanceManager {
    private final byte[] serializedFnInfo;
    private final ConcurrentLinkedQueue<DoFnInfo<?, ?>> fns;

    private ConcurrentQueueInstanceManager(DoFnInfo<?, ?> info) {
      this.serializedFnInfo = SerializableUtils.serializeToByteArray(info);
      fns = new ConcurrentLinkedQueue<>();
    }

    @Override
    public DoFnInfo<?, ?> peek() throws Exception {
      DoFnInfo<?, ?> fn = fns.peek();
      if (fn == null) {
        fn = deserializeCopy();
        fns.offer(fn);
      }
      return fn;
    }

    @Override
    public DoFnInfo<?, ?> get() throws Exception {
      DoFnInfo<?, ?> fn = fns.poll();
      if (fn == null) {
        fn = deserializeCopy();
      }
      return fn;
    }

    private DoFnInfo<?, ?> deserializeCopy() throws Exception {
      DoFnInfo<?, ?> fn;
      fn = (DoFnInfo<?, ?>) SerializableUtils.deserializeFromByteArray(serializedFnInfo, null);
      DoFnInvokers.invokerFor(fn.getDoFn()).invokeSetup();
      return fn;
    }

    @Override
    public void complete(DoFnInfo<?, ?> fnInfo) throws Exception {
      if (fnInfo != null) {
        fns.offer(fnInfo);
      }
    }

    @Override
    public void abort(DoFnInfo<?, ?> fnInfo) throws Exception {
      if (fnInfo != null && fnInfo.getDoFn() != null) {
        DoFnInvokers.invokerFor(fnInfo.getDoFn()).invokeTeardown();
      }
    }
  }

  private static class SingleInstanceManager implements DoFnInstanceManager {
    private final DoFnInfo<?, ?> info;

    private SingleInstanceManager(DoFnInfo<?, ?> info) {
      this.info = info;
    }

    @Override
    public DoFnInfo<?, ?> peek() throws Exception {
      return info;
    }

    @Override
    public DoFnInfo<?, ?> get() throws Exception {
      return info;
    }

    @Override
    public void complete(DoFnInfo<?, ?> fnInfo) throws Exception {}

    @Override
    public void abort(DoFnInfo<?, ?> fnInfo) throws Exception {}
  }
}
