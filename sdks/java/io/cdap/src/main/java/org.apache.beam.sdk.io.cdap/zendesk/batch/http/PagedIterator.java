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
package org.apache.beam.sdk.io.cdap.zendesk.batch.http;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.cdap.zendesk.batch.ZendeskBatchSourceConfig;
import org.apache.beam.sdk.io.cdap.zendesk.common.ObjectType;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

/** Iterable for Zendesk page response. */
@SuppressWarnings("rawtypes")
public class PagedIterator implements Iterator<String>, Closeable {

  private static final Pattern RESTRICTED_PATTERN = Pattern.compile("%2B", Pattern.LITERAL);
  private static final String NEXT_PAGE = "next_page";
  private static final String END_TIME = "end_time";
  private static final String COUNT = "count";
  private static final String COMMENT = "Comment";
  private static final int INCREMENTAL_EXPORT_MAX_COUNT_BY_REQUEST = 1000;
  private static final long FIVE_MINUTES = TimeUnit.MINUTES.toMillis(5);

  private static final Gson GSON = new GsonBuilder().create();

  private final ZendeskBatchSourceConfig config;
  private final CloseableHttpClient httpClient;
  private final HttpClientContext httpClientContext;
  private final ObjectType objectType;

  private Iterator<String> current;
  private String nextPage;

  /**
   * Constructor for PagedIterator object.
   *
   * @param config The batch source config
   * @param objectType The object type
   * @param subdomain The subdomain name
   */
  public PagedIterator(ZendeskBatchSourceConfig config, ObjectType objectType, String subdomain) {
    this(config, objectType, subdomain, null);
  }

  /**
   * Constructor for PagedIterator object.
   *
   * @param config The batch source config
   * @param objectType The object type
   * @param subdomain The subdomain name
   * @param entityId The entity id
   */
  public PagedIterator(
      ZendeskBatchSourceConfig config, ObjectType objectType, String subdomain, Long entityId) {
    this.config = config;
    this.objectType = objectType;

    String firstPage = HttpUtil.createFirstPageUrl(config, objectType, subdomain, entityId);
    this.httpClient = HttpUtil.createHttpClient(config);
    this.httpClientContext = HttpUtil.createHttpContext(config, firstPage);
    this.nextPage = firstPage;
  }

  @Override
  public boolean hasNext() {
    if (current == null || !current.hasNext()) {
      if (nextPage == null || nextPage.equalsIgnoreCase("null")) {
        return false;
      }

      Retryer<Map<String, Object>> retryer = HttpUtil.buildRetryer(config);
      try {
        Map<String, Object> responseMap = retryer.call(this::getResponseAsMap);
        nextPage = getNextPage(responseMap);
        current = getJsonValuesFromResponse(responseMap);
      } catch (ExecutionException | RetryException e) {
        throw new ConnectionTimeoutException(
            String.format(
                "Cannot create Zendesk connection for object: '%s'", objectType.getObjectName()),
            e);
      }
    }
    return current.hasNext();
  }

  @Override
  public String next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return current.next();
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  @VisibleForTesting
  Map<String, Object> getResponseAsMap() throws IOException {
    // replace out %2B with + due to API restriction
    URI uri = URI.create(RESTRICTED_PATTERN.matcher(nextPage).replaceAll("+"));
    try (CloseableHttpResponse response = httpClient.execute(new HttpGet(uri), httpClientContext)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode / 100 == 2) {
        String responseAsString =
            new String(EntityUtils.toByteArray(response.getEntity()), StandardCharsets.UTF_8);
        return (Map<String, Object>) GSON.fromJson(responseAsString, Map.class);
      }
      if (statusCode == 429) {
        throw new RateLimitException();
      }
      if (objectType == ObjectType.ARTICLE_COMMENTS
          || objectType == ObjectType.POST_COMMENTS
          || objectType == ObjectType.REQUESTS_COMMENTS) {
        return ImmutableMap.of(objectType.getResponseKey(), new ArrayList<>());
      }
      throw new HttpResponseException(statusCode, "No response.");
    }
  }

  @VisibleForTesting
  String getNextPage(Map<String, Object> responseMap) {
    if (!objectType.isBatch()) {
      return (String) responseMap.get(NEXT_PAGE);
    }

    String next = (String) responseMap.get(NEXT_PAGE);
    if (next == null) {
      return null;
    }

    // A request after five minutes ago will result in a 422 response from Zendesk.
    // Therefore, we stop pagination.
    Number endTime = (Number) responseMap.get(END_TIME);
    if (endTime == null
        || endTime.longValue() == 0
        || TimeUnit.SECONDS.toMillis(endTime.longValue())
            > System.currentTimeMillis() - FIVE_MINUTES) {
      return null;
    }

    // Taking into account documentation found at
    // https://developer.zendesk.com/rest_api/docs/core/incremental_export#polling-strategy
    Number count = (Number) responseMap.get(COUNT);
    if (count == null || count.intValue() < INCREMENTAL_EXPORT_MAX_COUNT_BY_REQUEST) {
      return null;
    }

    return next;
  }

  @VisibleForTesting
  Iterator<String> getJsonValuesFromResponse(Map<String, Object> responseMap) {
    List<Object> responseObjects = (List<Object>) responseMap.get(objectType.getResponseKey());
    if (objectType.getChildKey() == null) {
      return responseObjects.stream().map(this::objectMapToJsonString).iterator();
    }

    return responseObjects.stream()
        .flatMap(
            responseObject ->
                ((List<Object>) ((Map) responseObject).get(objectType.getChildKey())).stream())
        .filter(map -> COMMENT.equals(((Map) map).get("event_type")))
        .map(this::objectMapToJsonString)
        .iterator();
  }

  private String objectMapToJsonString(Object map) {
    Map objectMap = (Map) map;
    replaceKeys(objectMap, objectType.getObjectSchema());
    objectMap.put("object", objectType.getObjectName());
    return GSON.toJson(map);
  }

  @VisibleForTesting
  void replaceKeys(Map map, Schema schema) {
    if (map == null || map.isEmpty() || schema == null) {
      return;
    }
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      String underscoreName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
      if (!name.equals(underscoreName) && map.containsKey(underscoreName)) {
        map.put(name, map.remove(underscoreName));
      }
      Schema fieldSchema = field.getSchema();
      if (isRecord(fieldSchema)) {
        Map recordMap = (Map) map.get(name);
        Schema recordSchema = getRecordSchema(fieldSchema);
        replaceKeys(recordMap, recordSchema);
      }
    }
  }

  @VisibleForTesting
  boolean isRecord(Schema fieldSchema) {
    Schema.Type schemaType = fieldSchema.getType();
    return schemaType == Schema.Type.RECORD
        || (schemaType == Schema.Type.UNION
            && fieldSchema.getUnionSchemas().stream()
                .map(this::isRecord)
                .reduce(Boolean::logicalOr)
                .orElse(false));
  }

  @VisibleForTesting
  Schema getRecordSchema(Schema fieldSchema) {
    Schema.Type schemaType = fieldSchema.getType();
    if (schemaType == Schema.Type.RECORD) {
      return fieldSchema;
    }
    if (schemaType == Schema.Type.UNION) {
      return fieldSchema.getUnionSchemas().stream()
          .map(this::getRecordSchema)
          .filter(Objects::nonNull)
          .findFirst()
          .orElse(null);
    }
    return null;
  }
}
