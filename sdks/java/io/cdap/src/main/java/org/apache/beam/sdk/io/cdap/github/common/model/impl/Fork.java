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

/** Fork model for github. */
@SuppressWarnings("UnusedVariable")
public class Fork implements GitHubModel {

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

  @Key("keys_url")
  private String keysUrl;

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

  @Key("clone_url")
  private String cloneUrl;

  @Key("mirror_url")
  private String mirrorUrl;

  @Key("hooks_url")
  private String hooksUrl;

  @Key("svn_url")
  private String svnUrl;

  @Key private String homepage;
  @Key private String language;

  @Key("forks_count")
  private Integer forksCount;

  @Key("stargazers_count")
  private Integer stargazersCount;

  @Key("watchers_count")
  private Integer watchersCount;

  @Key private Long size;

  @Key("default_branch")
  private String defaultBranch;

  @Key("open_issues_count")
  private Integer openIssuesCount;

  @Key("is_template")
  private Boolean isTemplate;

  @Key private List<String> topics;

  @Key("has_issues")
  private Boolean hasIssues;

  @Key("has_projects")
  private Boolean hasProjects;

  @Key("has_wiki")
  private Boolean hasWiki;

  @Key("has_pages")
  private Boolean hasPages;

  @Key("has_downloads")
  private Boolean hasDownloads;

  @Key private Boolean archived;
  @Key private Boolean disabled;

  @Key("pushed_at")
  private String pushedAt;

  @Key("created_at")
  private String createdAt;

  @Key("updated_at")
  private String updatedAt;

  @Key private Permissions permissions;

  @Key("template_repository")
  private String templateRepository;

  @Key("subscribers_count")
  private Integer subscribersCount;

  @Key("network_count")
  private Integer networkCount;

  @Key private License license;

  /** Fork.License model */
  public static class License {
    @Key private String key;
    @Key private String name;

    @Key("spdx_id")
    private String spdxId;

    @Key private String url;

    @Key("node_id")
    private String nodeId;
  }

  /** Fork.Permissions model */
  public static class Permissions {
    @Key private Boolean pull;
    @Key private Boolean push;
    @Key private Boolean admin;
  }
}
