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

import com.google.api.services.storage.Storage;
import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TransportTest {

  @Test
  public void testUserAgentInGcsRequestHeaders() throws IOException {
    GcsOptions gcsOptions = PipelineOptionsFactory.as(GcsOptions.class);
    Storage storageClient = Transport.newStorageClient(gcsOptions).build();
    Storage.Objects.Get getObject = storageClient.objects().get("testbucket", "testobject");
    // An example of user agent string will be like
    // "TransportTest (GPN:Beam) Google-API-Java-Client/2.0.0"
    // For a valid user-agent string, a comment like "(GPN:Beam)" cannot be the first token.
    // https://www.rfc-editor.org/rfc/rfc7231#section-5.5.3
    assertThat(
        Arrays.asList(getObject.getRequestHeaders().getUserAgent().split(" "))
            .indexOf("(GPN:Beam)"),
        greaterThan(0));
  }
}
