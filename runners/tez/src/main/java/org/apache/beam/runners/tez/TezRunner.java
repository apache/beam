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
package org.apache.beam.runners.tez;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.tez.translation.TezPipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that translates the
 * pipeline to an Tez DAG and executes it on a Tez cluster.
 *
 */
public class TezRunner extends PipelineRunner<TezRunnerResult>{

  private static final Logger LOG = LoggerFactory.getLogger(TezClient.class);

  private final TezPipelineOptions options;

  private TezRunner(TezPipelineOptions options){
    this.options = options;
  }

  public static TezRunner fromOptions(PipelineOptions options) {
    TezPipelineOptions tezOptions = PipelineOptionsValidator.validate(TezPipelineOptions.class,options);
    return new TezRunner(tezOptions);
  }

  @Override
  public TezRunnerResult run(Pipeline pipeline) {
    //Setup Tez Local Config
    TezConfiguration config = new TezConfiguration();
    config.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    config.set("fs.default.name", "file:///");
    config.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    config.set(TezConfiguration.TEZ_TASK_LOG_LEVEL, "DEBUG");
    //TODO: Support Remote Tez Configuration

    final TezPipelineTranslator translator = new TezPipelineTranslator(options, config);
    final AtomicReference<DAG> tezDAG = new AtomicReference<>();
    DAG dag = DAG.create(options.getJobName());
    tezDAG.set(dag);
    translator.translate(pipeline, dag);

    TezClient client = TezClient.create("TezRun", config);
    try {
      client.start();
      client.submitDAG(dag);
    } catch (Exception e){
      e.printStackTrace();
    }

    return new TezRunnerResult(client);
  }
}