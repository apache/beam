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
package org.apache.beam.sdk.io.cdap.github.batch;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.cdap.github.common.GitHubRequestFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** InputFormat for mapreduce job, which provides a single split of data. */
@SuppressWarnings({"rawtypes", "StringSplitter"})
public class GithubInputFormat extends InputFormat {

  private static final Gson GSON = new GsonBuilder().create();

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();
    String configJson = conf.get(GithubFormatProvider.PROPERTY_CONFIG_JSON);
    GithubBatchSourceConfig config = GSON.fromJson(configJson, GithubBatchSourceConfig.class);

    String url = GitHubRequestFactory.generateFirstCallUrl(config);
    HttpRequest httpRequest =
        GitHubRequestFactory.buildRequest(url, config.getAuthorizationToken());
    HttpResponse response = httpRequest.execute();
    /**
     * TODO - https://issues.cask.co/browse/PLUGIN-382 Validate PaginationMetadata response for
     * Single page use cases.
     */
    Object paginationMetadata = response.getHeaders().get("Link");
    if (Objects.nonNull(paginationMetadata)) {
      String paginationUrls = (String) ((List<?>) paginationMetadata).get(0);
      String lastLink = getLastLink(paginationUrls);

      Integer totalPagesCount = getTotalPagesCount(lastLink);
      return IntStream.range(1, totalPagesCount)
          .mapToObj(
              pageNumber -> {
                String pageLink = transformLink(lastLink, pageNumber);
                return new GithubSplit(pageLink);
              })
          .collect(Collectors.toList());
    } else {
      return Collections.singletonList(new GithubSplit(url));
    }
  }

  @Override
  public RecordReader createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GithubFormatProvider.PROPERTY_CONFIG_JSON);
    GithubBatchSourceConfig config = GSON.fromJson(configJson, GithubBatchSourceConfig.class);
    return new GithubRecordReader(config, ((GithubSplit) inputSplit).getLink());
  }

  private String transformLink(String link, Integer pageNumber) {
    int indexOfLastParamValue = link.lastIndexOf("=");
    String substring = link.substring(0, indexOfLastParamValue + 1);
    return substring + pageNumber;
  }

  // TODO - Describe what this function does and how does paginationHeader Look like.
  // https://issues.cask.co/browse/PLUGIN-384

  private String getLastLink(String paginationHeader) {
    String[] links = paginationHeader.split(",");
    for (String link : links) {
      if (link.contains("last")) {
        String[] parts = link.split(";");
        String url = parts[0];
        return url.substring(2, url.length() - 1);
      }
    }
    return null;
  }

  private Integer getTotalPagesCount(String lastLink) {
    String[] split = lastLink.split("\\?");
    String paramsString = split[1];
    String[] params = paramsString.split("&");

    for (String param : params) {
      String[] keyValueString = param.split("=");
      if ("page".equals(keyValueString[0])) {
        return Integer.valueOf(keyValueString[1]);
      }
    }
    return null;
  }
}
