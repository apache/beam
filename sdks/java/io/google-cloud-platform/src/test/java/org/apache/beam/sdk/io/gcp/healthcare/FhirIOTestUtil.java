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

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1.model.HttpBody;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;

class FhirIOTestUtil {
  public static final String DEFAULT_TEMP_BUCKET = "temp-storage-for-healthcare-io-tests";

  private static Stream<String> readPrettyBundles(String resourcesPath) {
    Path resourceDir = Paths.get("build", "resources", "test", resourcesPath);
    String absolutePath = resourceDir.toFile().getAbsolutePath();
    File dir = new File(absolutePath);
    File[] fhirJsons = dir.listFiles();
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
        .map(String::new);
  }

  // Could generate more messages at scale using a tool like
  // https://synthetichealth.github.io/synthea/ if necessary chose not to avoid the dependency.
  static final List<String> DSTU2_PRETTY_BUNDLES =
      readPrettyBundles("DSTU2").collect(Collectors.toList());
  static final List<String> STU3_PRETTY_BUNDLES =
      readPrettyBundles("STU3").collect(Collectors.toList());
  static final List<String> R4_PRETTY_BUNDLES =
      readPrettyBundles("R4").collect(Collectors.toList());
  static final List<String> BUNDLE_PARSE_TEST_PRETTY_BUNDLES =
      readPrettyBundles("BUNDLE_PARSE_TEST").collect(Collectors.toList());

  static final Map<String, List<String>> BUNDLES;

  static {
    Map<String, List<String>> m = new HashMap<>();
    m.put("DSTU2", DSTU2_PRETTY_BUNDLES);
    m.put("STU3", STU3_PRETTY_BUNDLES);
    m.put("R4", R4_PRETTY_BUNDLES);
    m.put("BUNDLE_PARSE_TEST", BUNDLE_PARSE_TEST_PRETTY_BUNDLES);
    BUNDLES = Collections.unmodifiableMap(m);
  }

  /** Populate the test resources into the FHIR store and returns a list of resource IDs. */
  static List<String> executeFhirBundles(
      HealthcareApiClient client, String fhirStore, List<String> bundles)
      throws IOException, HealthcareHttpException {
    List<String> resourceNames = new ArrayList<>();
    for (String bundle : bundles) {
      HttpBody resp = client.executeFhirBundle(fhirStore, bundle);

      JsonObject jsonResponse = JsonParser.parseString(resp.toString()).getAsJsonObject();
      for (JsonElement entry : jsonResponse.getAsJsonArray("entry")) {
        String location =
            entry
                .getAsJsonObject()
                .getAsJsonObject("response")
                .getAsJsonPrimitive("location")
                .getAsString();
        String resourceName =
            location.substring(location.indexOf("project"), location.indexOf("/_history"));
        resourceNames.add(resourceName);
      }
    }
    return resourceNames;
  }

  public static void tearDownTempBucket() throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    HttpRequestInitializer requestInitializer =
        request -> {
          HttpHeaders requestHeaders = request.getHeaders();
          if (!credentials.hasRequestMetadata()) {
            return;
          }
          URI uri = null;
          if (request.getUrl() != null) {
            uri = request.getUrl().toURI();
          }
          Map<String, List<String>> credentialHeaders = credentials.getRequestMetadata(uri);
          if (credentialHeaders == null) {
            return;
          }
          for (Map.Entry<String, List<String>> entry : credentialHeaders.entrySet()) {
            String headerName = entry.getKey();
            List<String> requestValues = new ArrayList<>(entry.getValue());
            requestHeaders.put(headerName, requestValues);
          }
          request.setConnectTimeout(60000); // 1 minute connect timeout
          request.setReadTimeout(60000); // 1 minute read timeout
        };
    Storage storage =
        new Storage.Builder(new NetHttpTransport(), new GsonFactory(), requestInitializer).build();
    List<StorageObject> blobs = storage.objects().list(DEFAULT_TEMP_BUCKET).execute().getItems();
    if (blobs != null) {
      for (StorageObject blob : blobs) {
        storage.objects().delete(DEFAULT_TEMP_BUCKET, blob.getId());
      }
    }
  }
}
