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

package org.apache.beam.sdk.nexmark;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryModel;
import org.apache.beam.sdk.nexmark.queries.Query0;
import org.apache.beam.sdk.nexmark.queries.Query0Model;
import org.apache.beam.sdk.nexmark.queries.Query1;
import org.apache.beam.sdk.nexmark.queries.Query10;
import org.apache.beam.sdk.nexmark.queries.Query11;
import org.apache.beam.sdk.nexmark.queries.Query12;
import org.apache.beam.sdk.nexmark.queries.Query1Model;
import org.apache.beam.sdk.nexmark.queries.Query2;
import org.apache.beam.sdk.nexmark.queries.Query2Model;
import org.apache.beam.sdk.nexmark.queries.Query3;
import org.apache.beam.sdk.nexmark.queries.Query3Model;
import org.apache.beam.sdk.nexmark.queries.Query4;
import org.apache.beam.sdk.nexmark.queries.Query4Model;
import org.apache.beam.sdk.nexmark.queries.Query5;
import org.apache.beam.sdk.nexmark.queries.Query5Model;
import org.apache.beam.sdk.nexmark.queries.Query6;
import org.apache.beam.sdk.nexmark.queries.Query6Model;
import org.apache.beam.sdk.nexmark.queries.Query7;
import org.apache.beam.sdk.nexmark.queries.Query7Model;
import org.apache.beam.sdk.nexmark.queries.Query8;
import org.apache.beam.sdk.nexmark.queries.Query8Model;
import org.apache.beam.sdk.nexmark.queries.Query9;
import org.apache.beam.sdk.nexmark.queries.Query9Model;

/**
 * Queries and query models.
 */
public class NexmarkQueries {

  /**
   * Creates a NexmarkQuery.
   */
  public static NexmarkQuery createQuery(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      long now) {

    return createQueries(configuration, options, now).get(configuration.query);
  }

  private static List<NexmarkQuery> createQueries(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      long now) {

    return Arrays.asList(
        new Query0(configuration),
        new Query1(configuration),
        new Query2(configuration),
        new Query3(configuration),
        new Query4(configuration),
        new Query5(configuration),
        new Query6(configuration),
        new Query7(configuration),
        new Query8(configuration),
        new Query9(configuration),
        new Query10(configuration, options, now),
        new Query11(configuration),
        new Query12(configuration));
  }

  /**
   * Creates a NexmarkQuery model. Some queries don't have corresponding models.
   */
  public static NexmarkQueryModel createQueryModel(NexmarkConfiguration configuration) {
    return createModels(configuration).get(configuration.query);
  }

  private static List<NexmarkQueryModel> createModels(NexmarkConfiguration configuration) {
    return Arrays.asList(
        new Query0Model(configuration),
        new Query1Model(configuration),
        new Query2Model(configuration),
        new Query3Model(configuration),
        new Query4Model(configuration),
        new Query5Model(configuration),
        new Query6Model(configuration),
        new Query7Model(configuration),
        new Query8Model(configuration),
        new Query9Model(configuration),
        null,
        null,
        null);
  }
}
