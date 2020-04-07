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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Import.ContentStructure;

class FhirIOTestUtil {

  private static Stream<HttpBody> readPrettyBundles() {
    Path resourceDir = Paths.get("src", "test", "resources", "synthea_fhir_stu3_pretty");
    String absolutePath = resourceDir.toFile().getAbsolutePath();
    File dir = new File(absolutePath);
    File[] fhirJsons = dir.listFiles();
    assert fhirJsons != null;
    return Arrays.stream(fhirJsons)
        .map(File::toPath)
        .map(
            (Path path) -> {
              try {
                return Files.readAllBytes(path);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .map(String::new)
        .map(
            (String data) -> {
              HttpBody httpBody = new HttpBody();
              httpBody.setContentType(ContentStructure.BUNDLE_PRETTY.name());
              httpBody.setData(data);
              return httpBody;
            });
  }

  // Could generate more messages at scale using a tool like
  // https://synthetichealth.github.io/synthea/ if necessary chose not to avoid the dependency.

  static final List<HttpBody> PRETTY_BUNDLES = readPrettyBundles().collect(Collectors.toList());

  /** Populate the test resources into the FHIR store. */
  static void executeFhirBundles(HealthcareApiClient client, String fhirStore) throws IOException {
    for (HttpBody bundle : PRETTY_BUNDLES) {
      client.executeFhirBundle(fhirStore, bundle);
    }
  }
}
