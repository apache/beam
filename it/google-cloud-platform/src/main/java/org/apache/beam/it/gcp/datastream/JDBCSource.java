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
package org.apache.beam.it.gcp.datastream;

import com.google.protobuf.MessageOrBuilder;
import java.util.List;
import java.util.Map;

/** Parent class for JDBC Resources required for configuring Datastream. */
public abstract class JDBCSource {
  private final String hostname;
  private final String username;
  private final String password;
  private final int port;
  private final Map<String, List<String>> allowedTables;

  enum SourceType {
    ORACLE,
    MYSQL,
    POSTGRESQL,
  }

  JDBCSource(Builder<?> builder) {
    this.hostname = builder.hostname;
    this.username = builder.username;
    this.password = builder.password;
    this.port = builder.port;
    this.allowedTables = builder.allowedTables;
  }

  public abstract SourceType type();

  public abstract MessageOrBuilder config();

  public String hostname() {
    return this.hostname;
  }

  public String username() {
    return this.username;
  }

  public String password() {
    return this.password;
  }

  public int port() {
    return this.port;
  }

  public Map<String, List<String>> allowedTables() {
    return this.allowedTables;
  }

  public abstract static class Builder<T extends JDBCSource> {
    private String hostname;
    private String username;
    private String password;
    private int port;
    private Map<String, List<String>> allowedTables;

    public Builder(
        String hostname,
        String username,
        String password,
        int port,
        Map<String, List<String>> allowedTables) {
      this.hostname = hostname;
      this.username = username;
      this.password = password;
      this.port = port;
      this.allowedTables = allowedTables;
    }

    public abstract T build();
  }
}
