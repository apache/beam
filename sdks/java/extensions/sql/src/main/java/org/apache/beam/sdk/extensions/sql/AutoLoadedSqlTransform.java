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

import java.util.List;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PDone;

/**
 * A PTransform for executing a query from a list of DDL strings and a query string.
 *
 * <p>It loads all registered TableProviders, builtin functions and UDFs.
 */
@Experimental
public class AutoLoadedSqlTransform extends PTransform<PBegin, PDone> {
  private final List<String> ddls;
  private final String query;

  public AutoLoadedSqlTransform(List<String> ddls, String query) {
    this.ddls = ddls;
    this.query = query;
  }

  @Override
  public PDone expand(PBegin input) {
    MetaStore store = new InMemoryMetaStore();
    ServiceLoader.load(TableProvider.class).forEach(store::registerProvider);
    BeamSqlEnv env =
        BeamSqlEnv.builder(store)
            .autoLoadBuiltinFunctions()
            .autoLoadUserDefinedFunctions()
            .setPipelineOptions(input.getPipeline().getOptions())
            .build();
    ddls.forEach(env::executeDdl);
    BeamSqlRelUtils.toPCollection(input.getPipeline(), env.parseQuery(query));
    return PDone.in(input.getPipeline());
  }
}
