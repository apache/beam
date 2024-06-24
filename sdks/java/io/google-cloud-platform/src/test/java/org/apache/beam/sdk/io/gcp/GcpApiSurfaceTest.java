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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
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
            .pruningPattern("org[.]checkerframework[.].*[.]qual[.].*")
            // Quick check packages, exposed only in testing
            .pruningPattern("com[.]pholser[.]junit[.]quickcheck[.].*")
            .pruningPattern("org[.]javaruntype[.]type[.].*")
            // ------
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
            classesInPackage("com.google.api.gax.retrying"),
            classesInPackage("com.google.api.gax.longrunning"),
            classesInPackage("com.google.api.gax.rpc"),
            classesInPackage("com.google.api.gax.grpc"),
            classesInPackage("com.google.api.gax.tracing"),
            classesInPackage("com.google.api.gax.core"),
            classesInPackage("com.google.api.gax.batching"),
            classesInPackage("com.google.api.gax.paging"),
            classesInPackage("com.google.api.services.bigquery.model"),
            classesInPackage("com.google.api.services.healthcare"),
            classesInPackage("com.google.auth"),
            classesInPackage("com.google.bigtable.v2"),
            classesInPackage("com.google.bigtable.admin.v2"),
            classesInPackage("com.google.cloud"),
            classesInPackage("com.google.common.collect"),
            classesInPackage("com.google.cloud.bigquery.storage.v1"),
            classesInPackage("com.google.cloud.bigtable.config"),
            classesInPackage("com.google.iam.v1"),
            classesInPackage("com.google.spanner.v1"),
            classesInPackage("com.google.pubsub.v1"),
            classesInPackage("com.google.cloud.pubsublite"),
            Matchers.equalTo(com.google.api.gax.rpc.ApiException.class),
            Matchers.<Class<?>>equalTo(com.google.api.gax.rpc.StatusCode.class),
            Matchers.<Class<?>>equalTo(com.google.api.resourcenames.ResourceName.class),
            Matchers.<Class<?>>equalTo(com.google.common.base.Function.class),
            Matchers.<Class<?>>equalTo(com.google.common.base.Optional.class),
            Matchers.<Class<?>>equalTo(com.google.common.base.Supplier.class),
            Matchers.<Class<?>>equalTo(com.google.api.gax.rpc.StatusCode.Code.class),
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
            // BatchWrite for Firestore returns individual Status for each write attempted, in the
            // case of a failure and using the dead letter queue the Status is returned as part of
            // the WriteFailure
            Matchers.<Class<?>>equalTo(com.google.rpc.Status.class),
            Matchers.<Class<?>>equalTo(com.google.rpc.Status.Builder.class),
            Matchers.<Class<?>>equalTo(com.google.rpc.StatusOrBuilder.class),
            classesInPackage("com.google.cloud.spanner"),
            classesInPackage("com.google.longrunning"),
            classesInPackage("com.google.spanner.admin.database.v1"),
            classesInPackage("com.google.datastore.v1"),
            classesInPackage("com.google.firestore.v1"),
            classesInPackage("com.google.protobuf"),
            classesInPackage("com.google.rpc"),
            classesInPackage("com.google.type"),
            classesInPackage("com.fasterxml.jackson.annotation"),
            classesInPackage("com.fasterxml.jackson.core"),
            classesInPackage("com.fasterxml.jackson.databind"),
            classesInPackage("io.grpc"),
            classesInPackage("java"),
            classesInPackage("javax"),
            classesInPackage("org.apache.avro"),
            classesInPackage("org.apache.beam"),
            classesInPackage("org.joda.time"),
            classesInPackage("org.threeten.bp"),
            classesInPackage("com.google.gson"));

    assertThat(apiSurface, containsOnlyClassesMatching(allowedClasses));
  }
}
