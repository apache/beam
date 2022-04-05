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
package org.apache.beam.sdk.io.cdap.hubspot.common;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.format.StructuredRecord;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

/** Helper class to incorporate Hubspot api interaction. */
@SuppressWarnings("UnusedVariable")
public class HubspotHelper {

  private static final int MAX_TRIES = 3;

  /** Number of objects in one page to pull. */
  public static final String PAGE_SIZE = "100";

  /**
   * Return the instance of HubspotPage.
   *
   * @param sourceHubspotConfig the source hubspot config
   * @param offset the offset is string type
   * @return the instance of HubspotPage
   * @throws IOException on issues with data reading
   */
  public HubspotPage getHubspotPage(SourceHubspotConfig sourceHubspotConfig, String offset)
      throws IOException {
    HttpGet request = getRequest(sourceHubspotConfig, offset);

    CloseableHttpResponse response = downloadPage(sourceHubspotConfig, request);
    HttpEntity entity = response.getEntity();
    String result;
    if (entity != null) {
      result = EntityUtils.toString(entity);
      return parseJson(sourceHubspotConfig, result);
    }
    return null;
  }

  private CloseableHttpResponse downloadPage(
      SourceHubspotConfig sourceHubspotConfig, HttpGet request) throws IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    CloseableHttpClient client = httpClientBuilder.build();

    int count = 0;
    while (true) {
      CloseableHttpResponse response = client.execute(request);
      StatusLine statusLine = response.getStatusLine();
      if (statusLine.getStatusCode() >= 300) {
        if (500 > statusLine.getStatusCode() && statusLine.getStatusCode() >= 400) {
          throw new IOException(statusLine.getReasonPhrase());
        }
        if (++count == MAX_TRIES) {
          throw new IOException(statusLine.getReasonPhrase());
        }
      }
      return response;
    }
  }

  private HttpGet getRequest(SourceHubspotConfig sourceHubspotConfig, String offset) {
    URI uri = null;
    try {
      URIBuilder b = new URIBuilder(getEndpoint(sourceHubspotConfig));
      if (sourceHubspotConfig.apiKey != null) {
        b.addParameter("hapikey", sourceHubspotConfig.apiKey);
      }
      if (sourceHubspotConfig.startDate != null) {
        b.addParameter("start", sourceHubspotConfig.startDate);
      }
      if (sourceHubspotConfig.endDate != null) {
        b.addParameter("end", sourceHubspotConfig.endDate);
      }
      for (String filter : sourceHubspotConfig.getFilters()) {
        b.addParameter("f", filter);
      }
      if (getLimitPropertyName(sourceHubspotConfig) != null) {
        b.addParameter(getLimitPropertyName(sourceHubspotConfig), PAGE_SIZE);
      }
      if (offset != null && getOffsetPropertyName(sourceHubspotConfig) != null) {
        b.addParameter(getOffsetPropertyName(sourceHubspotConfig), offset);
      }
      uri = b.build();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Can't build valid uri", e);
    }
    return new HttpGet(uri);
  }

  private HubspotPage parseJson(SourceHubspotConfig sourceHubspotConfig, String json)
      throws IOException {
    JsonElement root = new JsonParser().parse(json);
    JsonObject jsonObject = root.getAsJsonObject();
    List<JsonElement> hubspotObjects = new ArrayList<>();
    String objectApiName = getObjectApiName(sourceHubspotConfig);
    if (objectApiName != null) {
      JsonElement jsonObjects = jsonObject.get(objectApiName);
      if (jsonObjects != null && jsonObjects.isJsonArray()) {
        JsonArray jsonObjectsArray = jsonObjects.getAsJsonArray();
        for (JsonElement jsonElement : jsonObjectsArray) {
          hubspotObjects.add(jsonElement);
        }
      } else {
        throw new IOException(
            String.format(
                "Not expected JSON response format, '%s' element not found or wrong type",
                objectApiName));
      }
    } else {
      hubspotObjects.add(root);
    }
    Boolean hasNext = null;
    String moreApiName = getMoreApiName(sourceHubspotConfig);
    if (moreApiName != null) {
      JsonElement hasNextElement = jsonObject.get(moreApiName);
      if (hasNextElement != null) {
        hasNext = hasNextElement.getAsBoolean();
      } else {
        throw new IOException(
            String.format(
                "Not expected JSON response format, '%s' element not found or wrong type",
                moreApiName));
      }
    }
    String offset = null;
    String offsetApiName = getOffsetApiName(sourceHubspotConfig);
    if (offsetApiName != null) {
      JsonElement offsetElement = jsonObject.get(offsetApiName);
      if (offsetElement != null) {
        offset = offsetElement.getAsString();
        JsonElement totalElement = jsonObject.get("total");
        if (hasNext == null && totalElement != null) {
          hasNext = !offset.equals(totalElement.getAsString()) && !offset.equals("0");
        }
      } else {
        throw new IOException(
            String.format(
                "Not expected JSON response format, '%s' element not found or wrong type",
                offsetApiName));
      }
    }
    return new HubspotPage(hubspotObjects, sourceHubspotConfig, offset, hasNext);
  }

  @Nullable
  private String getLimitPropertyName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS:
      case RECENT_COMPANIES:
      case COMPANIES:
      case CONTACTS:
        return "count";
      case EMAIL_EVENTS:
      case EMAIL_SUBSCRIPTION:
      case DEALS:
      case MARKETING_EMAIL:
      case ANALYTICS:
        return "limit";
      case DEAL_PIPELINES:
      case PRODUCTS:
      case TICKETS:
      default:
        return null;
    }
  }

  @Nullable
  private String getOffsetPropertyName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS:
      case EMAIL_EVENTS:
      case RECENT_COMPANIES:
      case COMPANIES:
      case DEALS:
      case MARKETING_EMAIL:
      case PRODUCTS:
      case TICKETS:
      case ANALYTICS:
      case EMAIL_SUBSCRIPTION:
        return "offset";
      case CONTACTS:
        return "vidOffset";
      case DEAL_PIPELINES:
      default:
        return null;
    }
  }

  @Nullable
  private String getOffsetApiName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS:
      case EMAIL_EVENTS:
      case RECENT_COMPANIES:
      case COMPANIES:
      case DEALS:
      case EMAIL_SUBSCRIPTION:
      case MARKETING_EMAIL:
      case PRODUCTS:
      case TICKETS:
        return "offset";
      case ANALYTICS:
        if (sourceHubspotConfig.getTimePeriod() != null
            && sourceHubspotConfig.getTimePeriod().equals(TimePeriod.TOTAL)) {
          return "offset";
        }
        return null;
      case CONTACTS:
        return "vid-offset";
      case DEAL_PIPELINES:
      default:
        return null;
    }
  }

  @Nullable
  private String getMoreApiName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS:
      case CONTACTS:
      case COMPANIES:
        return "has-more";
      case EMAIL_EVENTS:
      case RECENT_COMPANIES:
      case DEALS:
      case EMAIL_SUBSCRIPTION:
      case PRODUCTS:
      case TICKETS:
        return "hasMore";
      case DEAL_PIPELINES:
      case MARKETING_EMAIL:
      case ANALYTICS:
      default:
        return null;
    }
  }

  @Nullable
  private String getObjectApiName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS:
        return "lists";
      case CONTACTS:
        return "contacts";
      case EMAIL_EVENTS:
        return "events";
      case EMAIL_SUBSCRIPTION:
        return "timeline";
      case RECENT_COMPANIES:
      case DEAL_PIPELINES:
        return "results";
      case COMPANIES:
        return "companies";
      case DEALS:
        return "deals";
      case MARKETING_EMAIL:
      case PRODUCTS:
      case TICKETS:
        return "objects";
      case ANALYTICS:
        if (sourceHubspotConfig.getTimePeriod() != null
            && sourceHubspotConfig.getTimePeriod().equals(TimePeriod.TOTAL)) {
          return "breakdowns";
        }
        return null;
      default:
        return null;
    }
  }

  /**
   * Reurns the complete url as string.
   *
   * @param sourceHubspotConfig the source hubspot config
   * @return the complete url as string
   */
  @Nullable
  public String getEndpoint(SourceHubspotConfig sourceHubspotConfig) {
    String apiServerUrl = sourceHubspotConfig.getApiServerUrl();
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS:
        return String.format("%s/contacts/v1/lists", apiServerUrl);
      case CONTACTS:
        return String.format("%s/contacts/v1/lists/all/contacts/all", apiServerUrl);
      case EMAIL_EVENTS:
        return String.format("%s/email/public/v1/events", apiServerUrl);
      case EMAIL_SUBSCRIPTION:
        return String.format("%s/email/public/v1/subscriptions/timeline", apiServerUrl);
      case RECENT_COMPANIES:
        return String.format("%s/companies/v2/companies/recent/modified", apiServerUrl);
      case COMPANIES:
        return String.format("%s/companies/v2/companies/paged", apiServerUrl);
      case DEALS:
        return String.format("%s/deals/v1/deal/paged", apiServerUrl);
      case DEAL_PIPELINES:
        return String.format("%s/crm-pipelines/v1/pipelines/deals", apiServerUrl);
      case MARKETING_EMAIL:
        return String.format("%s/marketing-emails/v1/emails", apiServerUrl);
      case PRODUCTS:
        return String.format("%s/crm-objects/v1/objects/products/paged", apiServerUrl);
      case TICKETS:
        return String.format("%s/crm-objects/v1/objects/tickets/paged", apiServerUrl);
      case ANALYTICS:
        return String.format(
            "%s/analytics/v2/reports/%s/%s",
            apiServerUrl,
            sourceHubspotConfig.getReportEndpoint().getStringValue(),
            sourceHubspotConfig.getTimePeriod().getStringValue());
      default:
        return null;
    }
  }

  /**
   * Returns the StructuredRecord.
   *
   * @param value the value is string type
   * @param config the source hubspot config
   * @return the StructuredRecord
   */
  public static StructuredRecord transform(String value, SourceHubspotConfig config) {
    StructuredRecord.Builder builder = StructuredRecord.builder(config.getSchema());
    builder.set("objectType", config.objectType);
    builder.set("object", value);
    return builder.build();
  }
}
