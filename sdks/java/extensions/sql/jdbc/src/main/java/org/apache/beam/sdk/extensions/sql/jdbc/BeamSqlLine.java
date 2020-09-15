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
package org.apache.beam.sdk.extensions.sql.jdbc;

import static org.apache.beam.sdk.extensions.sql.impl.JdbcDriver.CONNECT_STRING_PREFIX;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.checkerframework.checker.nullness.qual.Nullable;
import sqlline.SqlLine;
import sqlline.SqlLine.Status;

/** {@link BeamSqlLine} provides default arguments to SqlLine. */
public class BeamSqlLine {

  private static final String NICKNAME = "BeamSQL";

  public static void main(String[] args) throws IOException {
    runSqlLine(args, null, System.out, System.err);
  }

  private static String[] checkConnectionArgs(String[] args) {
    List<String> argsList = new ArrayList<>(Arrays.asList(args));

    if (!argsList.contains("-nn")) {
      argsList.add("-nn");
      argsList.add(NICKNAME);
    }

    if (!argsList.contains("-u")) {
      argsList.add("-u");
      argsList.add(CONNECT_STRING_PREFIX);
    }

    return argsList.toArray(new String[argsList.size()]);
  }

  /** Nullable InputStream is being handled inside sqlLine.begin. */
  @SuppressWarnings("argument.type.incompatible")
  static Status runSqlLine(
      String[] args,
      @Nullable InputStream inputStream,
      @Nullable OutputStream outputStream,
      @Nullable OutputStream errorStream)
      throws IOException {
    String[] modifiedArgs = checkConnectionArgs(args);
    SqlLine sqlLine = new SqlLine();

    if (outputStream != null) {
      sqlLine.setOutputStream(new PrintStream(outputStream, false, Charsets.UTF_8.name()));
    }

    if (errorStream != null) {
      sqlLine.setErrorStream(new PrintStream(errorStream, false, Charsets.UTF_8.name()));
    }

    return sqlLine.begin(modifiedArgs, inputStream, true);
  }
}
