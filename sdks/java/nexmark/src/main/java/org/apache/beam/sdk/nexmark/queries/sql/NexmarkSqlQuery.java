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
package org.apache.beam.sdk.nexmark.queries.sql;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/** Executor for Nexmark queries. Allows to decouple from NexmarkQuery and test independently. */
public class NexmarkSqlQuery<T extends KnownSize> extends NexmarkQuery {

  private PTransform<PCollection<Event>, PCollection<T>> queryTransform;

  public NexmarkSqlQuery(
      NexmarkConfiguration configuration,
      PTransform<PCollection<Event>, PCollection<T>> queryTransform) {
    super(configuration, queryTransform.getName());
    this.queryTransform = queryTransform;
  }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
    PCollection<? extends KnownSize> resultRecordsSizes = events.apply(queryTransform);

    return NexmarkUtils.castToKnownSize(name, resultRecordsSizes);
  }
}
