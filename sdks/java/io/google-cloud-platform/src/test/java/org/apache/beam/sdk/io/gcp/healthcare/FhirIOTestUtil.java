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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import org.apache.beam.sdk.transforms.SerializableFunction;

class FhirIOTestUtil {
  public static final String DEFAULT_TEMP_BUCKET = "temp-storage-for-healthcare-io-tests";

  public static class ExtractIDSearchQuery implements SerializableFunction<String, String> {
    private ObjectMapper mapper;

    ExtractIDSearchQuery() {
      mapper = new ObjectMapper();
    }

    @Override
    public String apply(String resource) {
      try {
        Map<String, String> map =
            mapper.readValue(resource.getBytes(StandardCharsets.UTF_8), Map.class);
        String id = map.get("id");
        return String.format("_id=%s", id);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class ExtractIDSearchParams
      implements SerializableFunction<String, Map<String, String>> {
    private ObjectMapper mapper;

    ExtractIDSearchParams() {
      mapper = new ObjectMapper();
    }

    @Override
    public Map<String, String> apply(String resource) {
      Map<String, String> searchParams = new HashMap<>();
      try {
        Map<String, String> map =
            mapper.readValue(resource.getBytes(StandardCharsets.UTF_8), Map.class);
        String id = map.get("id");
        searchParams.put("_id", id);
        return searchParams;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class GetByKey implements SerializableFunction<String, String> {
    private final String key;
    private ObjectMapper mapper;

    public GetByKey(String key) {
      this.key = key;
      mapper = new ObjectMapper();
    }

    @Override
    public String apply(String resource) {
      try {
        Map<String, String> map =
            mapper.readValue(resource.getBytes(StandardCharsets.UTF_8), Map.class);
        return map.get(key);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // TODO read initial resources function.
  // TODO read update resources function.
  // TODO spot check resource update utility.

  private static Stream<String> readAllTestResources(String subDir, String version) {
    Path resourceDir = Paths.get("build", "resources", "test", subDir, version);
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

  private static List<String> readPrettyBundles(String version) {
    return readAllTestResources("transactional_bundles", version).collect(Collectors.toList());
  }

  private static List<String> readResources(String version) {
    return readAllTestResources("resources", version) // stream of file contents
        .map((String x) -> x.split("\\r?\\n")) // split lines
        .flatMap(Arrays::stream) // flatten lines for all files
        .collect(Collectors.toList());
  }
  // Could generate more messages at scale using a tool like
  // https://synthetichealth.github.io/synthea/ if necessary chose not to avoid the dependency.
  static final List<String> DSTU2_PRETTY_BUNDLES = readPrettyBundles("DSTU2");
  static final List<String> STU3_PRETTY_BUNDLES = readPrettyBundles("STU3");
  static final List<String> R4_PRETTY_BUNDLES = readPrettyBundles("R4");

  static final Map<String, List<String>> BUNDLES;

  static {
    Map<String, List<String>> m = new HashMap<>();
    m.put("DSTU2", DSTU2_PRETTY_BUNDLES);
    m.put("STU3", STU3_PRETTY_BUNDLES);
    m.put("R4", R4_PRETTY_BUNDLES);
    BUNDLES = Collections.unmodifiableMap(m);
  }

  // Could generate more messages at scale using a tool like
  // https://synthetichealth.github.io/synthea/ if necessary chose not to avoid the dependency.
  static final List<String> DSTU2_RESOURCES = readResources("DSTU2");
  static final List<String> STU3_RESOURCES = readResources("STU3");
  static final List<String> R4_RESOURCES = readResources("R4");

  static final Map<String, List<String>> RESOURCES;

  static {
    Map<String, List<String>> m = new HashMap<>();
    m.put("DSTU2", DSTU2_RESOURCES);
    m.put("STU3", STU3_RESOURCES);
    m.put("R4", R4_RESOURCES);
    RESOURCES = Collections.unmodifiableMap(m);
  }

  /** Populate the test resources into the FHIR store and returns a list of resource IDs. */
  static void executeFhirBundles(HealthcareApiClient client, String fhirStore, List<String> bundles)
      throws IOException, HealthcareHttpException {
    for (String bundle : bundles) {
      client.executeFhirBundle(fhirStore, bundle);
    }
  }

  /** Populate the test resources into the FHIR store and returns a list of resource IDs. */
  static void createFhirResources(
      HealthcareApiClient client, String fhirStore, List<String> resources)
      throws IOException, HealthcareHttpException {
    GetByKey getByKey = new GetByKey("resourceType");
    ExtractIDSearchQuery extractIDSearchQuery = new ExtractIDSearchQuery();
    for (String resource : resources) {
      client.fhirCreate(
          fhirStore, getByKey.apply(resource), resource, extractIDSearchQuery.apply(resource));
    }
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
        new Storage.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer)
            .build();
    List<StorageObject> blobs = storage.objects().list(DEFAULT_TEMP_BUCKET).execute().getItems();
    if (blobs != null) {
      for (StorageObject blob : blobs) {
        storage.objects().delete(DEFAULT_TEMP_BUCKET, blob.getId());
      }
    }
  }
}
