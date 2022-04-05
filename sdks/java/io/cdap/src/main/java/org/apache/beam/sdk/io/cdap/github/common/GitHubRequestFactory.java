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
package org.apache.beam.sdk.io.cdap.github.common;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import java.io.IOException;
import org.apache.beam.sdk.io.cdap.github.batch.GithubBatchSourceConfig;

/** Helper class to create GitHub data requests. */
public class GitHubRequestFactory {

  public static final Integer DEFAULT_PAGE_SIZE = 100;

  private static HttpRequestFactory requestFactory =
      new NetHttpTransport()
          .createRequestFactory(
              (HttpRequest request) ->
                  request.setParser(new JsonObjectParser(GsonFactory.getDefaultInstance())));

  public static String generateFirstCallUrl(GithubBatchSourceConfig config) {
    String host = config.getHostname() != null ? config.getHostname() : "https://api.github.com";
    return host
        + "/repos"
        + "/"
        + config.getRepoOwner()
        + "/"
        + config.getRepoName()
        + "/"
        + getPathByDatasetName(config.getDatasetName())
        + "?per_page="
        + DEFAULT_PAGE_SIZE;
  }

  public static HttpRequest buildRequest(String url, String authToken) throws IOException {
    HttpRequest httpRequest = requestFactory.buildGetRequest(new GenericUrl(url));
    addHeaders(httpRequest, authToken);
    return httpRequest;
  }

  private static void addHeaders(HttpRequest httpRequest, String authToken) {
    httpRequest.getHeaders().setAuthorization("token " + authToken);
    httpRequest.getHeaders().setUserAgent("curl/7.37.0");
  }

  private static String getPathByDatasetName(String dataset) {
    switch (dataset) {
      case "Branches":
        {
          return "branches";
        }
      case "Collaborators":
        {
          return "collaborators";
        }
      case "Comments":
        {
          return "comments";
        }
      case "Commits":
        {
          return "commits";
        }
      case "Contents":
        {
          return "contents";
        }
      case "Deploy Keys":
        {
          return "keys";
        }
      case "Deployments":
        {
          return "deployments";
        }
      case "Forks":
        {
          return "forks";
        }
      case "Invitations":
        {
          return "invitations";
        }
      case "Pages":
        {
          return "pages";
        }
      case "Releases":
        {
          return "releases";
        }
      case "Traffic:Referrers":
        {
          return "traffic/popular/referrers";
        }
      case "Webhooks":
        {
          return "hooks";
        }
      default:
        {
          throw new IllegalArgumentException(
              String.format("Unsupported dataset name %s!", dataset));
        }
    }
  }
}
