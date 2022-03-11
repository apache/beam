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
package org.apache.beam.sdk.io.cdap.github.common.model.impl.user;

import com.google.api.client.util.Key;
import org.apache.beam.sdk.io.cdap.github.common.model.GitHubModel;

/** User model for github. */
@SuppressWarnings("UnusedVariable")
public class User implements GitHubModel {

  @Key private Long id;

  @Key("node_id")
  private String nodeId;

  @Key("avatar_url")
  private String avatarUrl;

  @Key("gravatar_id")
  private String gravatarId;

  @Key private String url;

  @Key("html_url")
  private String htmlUrl;

  @Key("followers_url")
  private String followersUrl;

  @Key("following_url")
  private String followingUrl;

  @Key("gists_url")
  private String gistsUrl;

  @Key("starred_url")
  private String starredUrl;

  @Key("subscriptions_url")
  private String subscriptionsUrl;

  @Key("organizations_url")
  private String organizationsUrl;

  @Key("repos_url")
  private String reposUrl;

  @Key("events_url")
  private String eventsUrl;

  @Key("received_events_url")
  private String receivedEventsUrl;

  @Key private String type;

  @Key("site_admin")
  private Boolean siteAdmin;
}
