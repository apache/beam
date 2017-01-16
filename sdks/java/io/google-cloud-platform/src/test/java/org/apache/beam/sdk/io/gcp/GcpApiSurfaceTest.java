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
package org.apache.beam.sdk.io.gcp;

import static org.hamcrest.Matchers.equalTo;

import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.beam.sdk.util.ApiSurfaceVerification;
import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * API surface verification for {@link org.apache.beam.sdk.io.gcp}.
 */
@RunWith(JUnit4.class)
public class GcpApiSurfaceTest extends ApiSurfaceVerification {

  @Override
  protected Set<Matcher<? extends Class<?>>> allowedPackages() {
    return
        ImmutableSet.of(
            inPackage("com.google.api.client.json"),
            inPackage("com.google.api.client.util"),
            inPackage("com.google.api.services.bigquery.model"),
            inPackage("com.google.auth"),
            inPackage("com.google.bigtable.v2"),
            inPackage("com.google.cloud.bigtable.config"),
            equalTo(BigtableInstanceName.class),
            equalTo(BigtableTableName.class),
            // https://github.com/GoogleCloudPlatform/cloud-bigtable-client/pull/1056
            inPackage("com.google.common.collect"), // via Bigtable, PR above out to fix.
            inPackage("com.google.datastore.v1"),
            inPackage("com.google.protobuf"),
            inPackage("com.google.type"),
            inPackage("com.fasterxml.jackson.annotation"),
            inPackage("com.fasterxml.jackson.core"),
            inPackage("com.fasterxml.jackson.databind"),
            inPackage("io.grpc"),
            inPackage("java"),
            inPackage("javax"),
            inPackage("org.apache.beam"),
            inPackage("org.apache.commons.logging"), // via Bigtable
            inPackage("org.joda.time"));
  }
}
