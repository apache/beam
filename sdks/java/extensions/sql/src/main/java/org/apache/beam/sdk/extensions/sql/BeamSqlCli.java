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
package org.apache.beam.sdk.extensions.sql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** {@link BeamSqlCli} provides methods to execute Beam SQL with an interactive client. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamSqlCli {
  private BeamSqlEnv env;
  /** The store which persists all the table meta data. */
  private MetaStore metaStore;

  public BeamSqlCli metaStore(MetaStore metaStore) {
    return metaStore(metaStore, false, PipelineOptionsFactory.create());
  }

  public BeamSqlCli metaStore(
      MetaStore metaStore, boolean autoLoadUdfUdaf, PipelineOptions pipelineOptions) {
    this.metaStore = metaStore;
    BeamSqlEnv.BeamSqlEnvBuilder builder = BeamSqlEnv.builder(metaStore);
    if (autoLoadUdfUdaf) {
      builder.autoLoadUserDefinedFunctions();
    }
    builder.setPipelineOptions(pipelineOptions);
    this.env = builder.build();
    return this;
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  /** Returns a human readable representation of the query execution plan. */
  public String explainQuery(String sqlString) throws ParseException {
    return env.explain(sqlString);
  }

  /** Executes the given sql. */
  public void execute(String sqlString) throws ParseException {

    if (env.isDdl(sqlString)) {
      env.executeDdl(sqlString);
    } else {
      PipelineOptions options =
          BeamEnumerableConverter.createPipelineOptions(env.getPipelineOptions());
      options.setJobName("BeamPlanCreator");
      Pipeline pipeline = Pipeline.create(options);
      BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery(sqlString));
      pipeline.run();
    }
  }
}
