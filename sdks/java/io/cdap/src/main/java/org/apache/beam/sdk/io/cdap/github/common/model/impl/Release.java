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
package org.apache.beam.sdk.io.cdap.github.common.model.impl;

import com.google.api.client.util.Key;
import java.util.List;
import org.apache.beam.sdk.io.cdap.github.common.model.GitHubModel;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.user.User;

/** Release model for github. */
@SuppressWarnings("UnusedVariable")
public class Release implements GitHubModel {

  @Key private String url;

  @Key("html_url")
  private String htmlUrl;

  @Key("assets_url")
  private String assetsUrl;

  @Key("upload_url")
  private String uploadUrl;

  @Key("tarball_url")
  private String tarballUrl;

  @Key("zipball_url")
  private String zipballUrl;

  @Key private Long id;

  @Key("node_id")
  private String nodeId;

  @Key("tag_name")
  private String tagName;

  @Key("target_commitish")
  private String targetCommitish;

  @Key private String name;
  @Key private String body;
  @Key private Boolean draft;
  @Key private Boolean prerelease;

  @Key("created_at")
  private String createdAt;

  @Key("published_at")
  private String publishedAt;

  @Key private User author;
  @Key private List<Assert> assets;

  /** Release.Assert model */
  public static class Assert {
    @Key private String url;

    @Key("browser_download_url")
    private String browserDownloadUrl;

    @Key private Long id;

    @Key("node_id")
    private String nodeId;

    @Key private String name;
    @Key private String label;
    @Key private String state;

    @Key("content_type")
    private String contentType;

    @Key private Long size;

    @Key("download_count")
    private Integer downloadCount;

    @Key("created_at")
    private String createdAt;

    @Key("updated_at")
    private String updatedAt;

    @Key private User uploader;
  }
}
