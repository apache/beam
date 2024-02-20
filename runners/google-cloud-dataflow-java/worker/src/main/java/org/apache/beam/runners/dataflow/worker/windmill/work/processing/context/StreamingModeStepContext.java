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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.context;

import java.io.IOException;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

public interface StreamingModeStepContext {

  boolean issueSideInputFetch(PCollectionView<?> view, BoundedWindow w, SideInputState s);

  void addBlockingSideInput(Windmill.GlobalDataRequest blocked);

  void addBlockingSideInputs(Iterable<Windmill.GlobalDataRequest> blocked);

  StateInternals stateInternals();

  Iterable<Windmill.GlobalDataId> getSideInputNotifications();

  /** Writes the given {@code PCollectionView} data to a globally accessible location. */
  <T, W extends BoundedWindow> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<T> data,
      Coder<Iterable<T>> dataCoder,
      W window,
      Coder<W> windowCoder)
      throws IOException;
}
