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

import static org.apache.beam.sdk.util.ApiSurface.classesInPackage;
import static org.apache.beam.sdk.util.ApiSurface.containsOnlyClassesMatching;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Set;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.io.gcp.testing.BigqueryMatcher;
import org.apache.beam.sdk.util.ApiSurface;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** API surface verification for {@link org.apache.beam.sdk.io.gcp}. */
@RunWith(JUnit4.class)
public class GcpApiSurfaceTest {

  @Test
  public void testGcpApiSurface() throws Exception {

    final Package thisPackage = this.getClass().getPackage();
    final ClassLoader thisClassLoader = getClass().getClassLoader();

    final ApiSurface apiSurface =
        ApiSurface.ofPackage(thisPackage, thisClassLoader)
            .pruningPattern(BigqueryMatcher.class.getName())
            .pruningPattern(BigqueryClient.class.getName())
            .pruningPattern("org[.]apache[.]beam[.].*Test.*")
            .pruningPattern("org[.]apache[.]beam[.].*IT")
            .pruningPattern("java[.]lang.*")
            .pruningPattern("java[.]util.*");

    @SuppressWarnings("unchecked")
    final Set<Matcher<Class<?>>> allowedClasses =
        ImmutableSet.of(
            classesInPackage("com.google.api.core"),
            classesInPackage("com.google.api.client.googleapis"),
            classesInPackage("com.google.api.client.http"),
            classesInPackage("com.google.api.client.json"),
            classesInPackage("com.google.api.client.util"),
            classesInPackage("com.google.api.services.bigquery.model"),
            classesInPackage("com.google.api.services.healthcare"),
            classesInPackage("com.google.auth"),
            classesInPackage("com.google.bigtable.v2"),
            classesInPackage("com.google.cloud.bigquery.storage.v1"),
            classesInPackage("com.google.cloud.bigtable.config"),
            classesInPackage("com.google.spanner.v1"),
            classesInPackage("com.google.pubsub.v1"),
            classesInPackage("com.google.cloud.pubsublite"),
            Matchers.equalTo(com.google.api.gax.rpc.ApiException.class),
            Matchers.<Class<?>>equalTo(com.google.api.gax.rpc.StatusCode.class),
            Matchers.<Class<?>>equalTo(com.google.common.base.Function.class),
            Matchers.<Class<?>>equalTo(com.google.api.gax.rpc.StatusCode.Code.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.bigtable.grpc.BigtableClusterName.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.bigtable.grpc.BigtableInstanceName.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.bigtable.grpc.BigtableTableName.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.BaseServiceException.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.BaseServiceException.Error.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.BaseServiceException.ExceptionData.class),
            Matchers.<Class<?>>equalTo(
                com.google.cloud.BaseServiceException.ExceptionData.Builder.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.RetryHelper.RetryHelperException.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.grpc.BaseGrpcServiceException.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.ByteArray.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.Date.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.Timestamp.class),
            // TODO: remove the following classes once spanner updates APIs of AsyncResultSet:
            // https://github.com/googleapis/java-spanner/issues/410
            Matchers.<Class<?>>equalTo(com.google.common.collect.ImmutableCollection.class),
            Matchers.<Class<?>>equalTo(com.google.common.collect.ImmutableCollection.Builder.class),
            Matchers.<Class<?>>equalTo(com.google.common.collect.ImmutableList.class),
            Matchers.<Class<?>>equalTo(com.google.common.collect.ImmutableList.Builder.class),
            Matchers.<Class<?>>equalTo(com.google.common.collect.UnmodifiableIterator.class),
            Matchers.<Class<?>>equalTo(com.google.common.collect.UnmodifiableListIterator.class),
            classesInPackage("com.google.cloud.spanner"),
            classesInPackage("com.google.datastore.v1"),
            classesInPackage("com.google.protobuf"),
            classesInPackage("com.google.type"),
            classesInPackage("com.fasterxml.jackson.annotation"),
            classesInPackage("com.fasterxml.jackson.core"),
            classesInPackage("com.fasterxml.jackson.databind"),
            classesInPackage("io.grpc"),
            classesInPackage("java"),
            classesInPackage("javax"),
            classesInPackage("org.apache.avro"),
            classesInPackage("org.apache.beam"),
            classesInPackage("org.apache.commons.logging"),
            classesInPackage("org.codehaus.jackson"),
            classesInPackage("org.joda.time"));

    assertThat(apiSurface, containsOnlyClassesMatching(allowedClasses));
  }
}
