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
package org.apache.beam.sdk.extensions.gcp.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;

import com.google.api.client.http.HttpRequest;
import com.google.api.services.storage.Storage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TransportTest {

  @Test
  public void testUserAgentAndCustomAuditInGcsRequestHeaders() throws IOException {
    GcsOptions gcsOptions = PipelineOptionsFactory.as(GcsOptions.class);
    gcsOptions.setGcpCredential(new TestCredential());
    gcsOptions.setJobName("test-job");
    gcsOptions.setAppName("test-app");

    Storage storageClient = Transport.newStorageClient(gcsOptions).build();
    Storage.Objects.Get getObject = storageClient.objects().get("test-bucket", "test-object");
    HttpRequest request = getObject.buildHttpRequest();

    // An example of user agent string will be like
    // "test-app apache-beam/2.57.0.dev (GPN:Beam) Google-API-Java-Client/2.0.0"
    // For a valid user-agent string, a comment like "(GPN:Beam)" cannot be the first token.
    // https://www.rfc-editor.org/rfc/rfc7231#section-5.5.3
    assertThat(
        Arrays.asList(request.getHeaders().getUserAgent().split(" ")).indexOf("test-app"),
        greaterThanOrEqualTo(0));

    assertThat(
        Arrays.asList(request.getHeaders().getUserAgent().split(" "))
            .indexOf(String.format("apache-beam/%s", ReleaseInfo.getReleaseInfo().getSdkVersion())),
        greaterThan(0));

    assertThat(
        Arrays.asList(request.getHeaders().getUserAgent().split(" ")).indexOf("(GPN:Beam)"),
        greaterThan(0));

    // there should be one and only one custom audit entry for job name
    assertEquals(
        request.getHeaders().getHeaderStringValues("x-goog-custom-audit-job"),
        Collections.singletonList("test-job"));
  }
}
