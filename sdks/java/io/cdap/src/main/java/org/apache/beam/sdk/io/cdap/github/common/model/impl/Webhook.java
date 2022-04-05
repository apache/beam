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

/** Webhook model for github. */
@SuppressWarnings("UnusedVariable")
public class Webhook implements GitHubModel {

  @Key private String type;
  @Key private Long id;
  @Key private String name;
  @Key private Boolean active;
  @Key private List<String> events;
  @Key private Config config;

  @Key("updated_at")
  private String updatedAt;

  @Key("created_at")
  private String createdAt;

  @Key private String url;

  @Key("test_url")
  private String testUrl;

  @Key("ping_url")
  private String pingUrl;

  @Key private LastResponse lastResponse;

  /** Webhook.Config model */
  public static class Config {
    @Key("content_type")
    private String contentType;

    @Key("insecure_ssl")
    private String insecureSsl;

    @Key private String url;
  }

  /** Webhook.LastResponse model */
  public static class LastResponse {
    @Key private String code;
    @Key private String status;
    @Key private String message;
  }
}
