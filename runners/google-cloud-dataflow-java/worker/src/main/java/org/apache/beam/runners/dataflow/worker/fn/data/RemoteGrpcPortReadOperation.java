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
package org.apache.beam.runners.dataflow.worker.fn.data;

import java.io.Closeable;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link Operation} that uses the Beam Fn Data API to receive messages.
 *
 * <p>This {@link Operation} produces output potentially on a different thread then the input. This
 * will require downstream consumers to synchronize processing of elements produced by this
 * operation with any other outputs they receive.
 *
 * <p>This {@link Operation} supports restart.
 */
public class RemoteGrpcPortReadOperation<T> extends Operation {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteGrpcPortReadOperation.class);
  private final Coder<WindowedValue<T>> coder;
  private final FnDataService beamFnDataService;
  private final IdGenerator bundleIdSupplier;
  // Should only be set and cleared once per start/finish cycle in the start method and
  // finish method respectively.
  private String bundleId;
  private final String ptransformId;
  private InboundDataClient inboundDataClient;

  public RemoteGrpcPortReadOperation(
      FnDataService beamFnDataService,
      String ptransformId,
      IdGenerator bundleIdSupplier,
      Coder<WindowedValue<T>> coder,
      OutputReceiver[] receivers,
      OperationContext context) {
    super(receivers, context);
    this.coder = coder;
    this.beamFnDataService = beamFnDataService;
    this.bundleIdSupplier = bundleIdSupplier;
    this.ptransformId = ptransformId;
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      bundleId = bundleIdSupplier.getId();
      super.start();
      inboundDataClient =
          beamFnDataService.receive(
              LogicalEndpoint.data(bundleId, ptransformId), coder, this::consumeOutput);
    }
  }

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterFinish()) {
      LOG.debug("Waiting for instruction {} and transform {} to close.", bundleId, ptransformId);
      inboundDataClient.awaitCompletion();
      bundleId = null;
      super.finish();
    }
  }

  @Override
  public void abort() throws Exception {
    try (Closeable scope = context.enterAbort()) {
      inboundDataClient.cancel();
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }

  private void consumeOutput(WindowedValue<T> value) throws Exception {
    for (OutputReceiver receiver : receivers) {
      receiver.process(value);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ptransformId", ptransformId)
        .add("coder", coder)
        .add("bundleId", bundleId)
        .toString();
  }
}
