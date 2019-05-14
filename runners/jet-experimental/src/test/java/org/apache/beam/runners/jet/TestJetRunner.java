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
package org.apache.beam.runners.jet;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;

/** Slightly altered version of the Jet based runner, used in unit-tests. */
public class TestJetRunner extends PipelineRunner<PipelineResult> {

  private final JetTestInstanceFactory factory = new JetTestInstanceFactory();

  public static TestJetRunner fromOptions(PipelineOptions options) {
    return new TestJetRunner(options);
  }

  private final JetRunner delegate;

  private TestJetRunner(PipelineOptions options) {
    JetPipelineOptions jetPipelineOptions = options.as(JetPipelineOptions.class);
    jetPipelineOptions.setJetStartOwnCluster(false);

    this.delegate = JetRunner.fromOptions(options, factory::newClient);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    Collection<JetInstance> instances = initInstances(factory);
    System.out.println("Created " + instances.size() + " instances.");
    try {
      PipelineResult result = delegate.run(pipeline);
      if (result instanceof FailedRunningPipelineResults) {
        RuntimeException failureCause = ((FailedRunningPipelineResults) result).getCause();
        throw failureCause;
      }
      return result;
    } finally {
      System.out.println("Shutting down " + instances.size() + " instances...");
      factory.shutdownAll();
    }
  }

  private Collection<JetInstance> initInstances(JetTestInstanceFactory factory) {
    JetConfig config = new JetConfig();
    config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig().setMapName("map"));

    return Arrays.asList(factory.newMember(config), factory.newMember(config));
  }
}
