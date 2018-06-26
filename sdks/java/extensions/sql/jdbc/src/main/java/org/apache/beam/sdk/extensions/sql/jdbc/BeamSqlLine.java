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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import sqlline.SqlLine;

/** {@link BeamSqlLine} provides default arguments to SqlLine. */
public class BeamSqlLine {

  private static final String NICKNAME = "BeamSQL";

  public static void main(String[] args) throws IOException {

    // Until we learn otherwise, we expect to add -nn <nickname> -u <url>
    @Nullable String databaseUrl = null;
    @Nullable String nickname = null;

    // Provide -u and -nn only if they do not exist
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-u")) {
        databaseUrl = args[++i];
      } else if (args[i].equals("-nn")) {
        nickname = args[++i];
      }
    }

    ImmutableList.Builder wrappedArgs = ImmutableList.builder().addAll(Arrays.asList(args));
    if (databaseUrl == null) {
      wrappedArgs.add("-u").add(CONNECT_STRING_PREFIX);
    }
    if (nickname == null) {
      wrappedArgs.add("-nn").add(NICKNAME);
    }
    List<String> wrappedArgList = wrappedArgs.build();
    SqlLine.main(wrappedArgList.toArray(new String[wrappedArgList.size()]));
  }
}
