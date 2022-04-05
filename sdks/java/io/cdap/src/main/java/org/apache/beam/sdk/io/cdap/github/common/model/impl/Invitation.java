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
import org.apache.beam.sdk.io.cdap.github.common.model.GitHubModel;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.user.User;

/** Invitation model for github. */
@SuppressWarnings("UnusedVariable")
public class Invitation implements GitHubModel {

  @Key private Long id;
  @Key private Repository repository;
  @Key private User invitee;
  @Key private User inviter;
  @Key private String permissions;

  @Key("created_at")
  private String createdAt;

  @Key private String url;

  @Key("html_url")
  private String htmlUrl;

  /** Invitation.Repository model */
  public static class Repository {
    @Key private Long id;

    @Key("node_id")
    private String nodeId;

    @Key private String name;

    @Key("full_name")
    private String fullName;

    @Key private User owner;

    @Key("private")
    private Boolean isPrivate;

    @Key("html_url")
    private String htmlUrl;

    @Key private String description;
    @Key private Boolean fork;
    @Key private String url;

    @Key("archive_url")
    private String archiveUrl;

    @Key("assignees_url")
    private String assigneesUrl;

    @Key("blobs_url")
    private String blobsUrl;

    @Key("branches_url")
    private String branchesUrl;

    @Key("collaborators_url")
    private String collaboratorsUrl;

    @Key("comments_url")
    private String commentsUrl;

    @Key("commits_url")
    private String commitsUrl;

    @Key("compare_url")
    private String compareUrl;

    @Key("contents_url")
    private String contentsUrl;

    @Key("contributors_url")
    private String contributorsUrl;

    @Key("deployments_url")
    private String deploymentsUrl;

    @Key("downloads_url")
    private String downloadsUrl;

    @Key("events_url")
    private String eventsUrl;

    @Key("forks_url")
    private String forksUrl;

    @Key("git_commits_url")
    private String gitCommitsUrl;

    @Key("git_refs_url")
    private String gitRefsUrl;

    @Key("git_tags_url")
    private String gitTagsUrl;

    @Key("git_url")
    private String gitUrl;

    @Key("issue_comment_url")
    private String issueCommentUrl;

    @Key("issue_events_url")
    private String issueEventsUrl;

    @Key("issues_url")
    private String issuesUrl;

    @Key("labels_url")
    private String labelsUrl;

    @Key("languages_url")
    private String languagesUrl;

    @Key("merges_url")
    private String mergesUrl;

    @Key("milestones_url")
    private String milestonesUrl;

    @Key("notifications_url")
    private String notificationsUrl;

    @Key("pulls_url")
    private String pullsUrl;

    @Key("releases_url")
    private String releasesUrl;

    @Key("ssh_url")
    private String sshUrl;

    @Key("stargazers_url")
    private String stargazersUrl;

    @Key("statuses_url")
    private String statusesUrl;

    @Key("subscribers_url")
    private String subscribersUrl;

    @Key("subscription_url")
    private String subscriptionUrl;

    @Key("tags_url")
    private String tagsUrl;

    @Key("teams_url")
    private String teamsUrl;

    @Key("trees_url")
    private String treesUrl;
  }
}
