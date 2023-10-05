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
package org.apache.beam.it.jdbc;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Default class for the MySQL implementation of {@link AbstractJDBCResourceManager} abstract class.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is *
 * created when the container first spins up, if one is not given.
 *
 * <p>The class is thread-safe.
 */
public class MySQLResourceManager extends AbstractJDBCResourceManager<MySQLContainer<?>> {

  private static final String DEFAULT_MYSQL_CONTAINER_NAME = "mysql";

  // A list of available mySQL Docker image tags can be found at
  // https://hub.docker.com/_/mysql/tags?tab=tags
  private static final String DEFAULT_MYSQL_CONTAINER_TAG = "8.0.30";

  private MySQLResourceManager(Builder builder) {
    this(
        new MySQLContainer<>(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  MySQLResourceManager(MySQLContainer<?> container, Builder builder) {
    super(container, builder);
  }

  public static MySQLResourceManager.Builder builder(String testId) {
    return new MySQLResourceManager.Builder(testId);
  }

  @Override
  protected int getJDBCPort() {
    return MySQLContainer.MYSQL_PORT;
  }

  @Override
  public String getJDBCPrefix() {
    return "mysql";
  }

  /** Builder for {@link MySQLResourceManager}. */
  public static final class Builder extends AbstractJDBCResourceManager.Builder<MySQLContainer<?>> {

    public Builder(String testId) {
      super(testId, DEFAULT_MYSQL_CONTAINER_NAME, DEFAULT_MYSQL_CONTAINER_TAG);
    }

    @Override
    public MySQLResourceManager build() {
      return new MySQLResourceManager(this);
    }
  }
}
