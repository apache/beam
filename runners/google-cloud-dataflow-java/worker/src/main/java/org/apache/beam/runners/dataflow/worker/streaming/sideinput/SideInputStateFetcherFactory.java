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
package org.apache.beam.runners.dataflow.worker.streaming.sideinput;

import java.util.function.Function;
import org.apache.beam.runners.dataflow.options.DataflowStreamingPipelineOptions;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Factory class for generating {@link SideInputStateFetcher} instances that share a {@link
 * SideInputCache}.
 */
@Internal
public final class SideInputStateFetcherFactory {
  private final SideInputCache globalSideInputCache;

  private SideInputStateFetcherFactory(SideInputCache globalSideInputCache) {
    this.globalSideInputCache = globalSideInputCache;
  }

  public static SideInputStateFetcherFactory fromOptions(DataflowStreamingPipelineOptions options) {
    return new SideInputStateFetcherFactory(SideInputCache.create(options));
  }

  public SideInputStateFetcher createSideInputStateFetcher(
      Function<GlobalDataRequest, GlobalData> fetchGlobalDataFn) {
    return new SideInputStateFetcher(fetchGlobalDataFn, globalSideInputCache);
  }
}
