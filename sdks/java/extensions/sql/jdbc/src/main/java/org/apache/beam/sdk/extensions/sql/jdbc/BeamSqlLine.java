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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jline.TerminalFactory;
import jline.UnixTerminal;
import jline.internal.Log;
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
  @SuppressWarnings("argument")
  static Status runSqlLine(
      String[] args,
      @Nullable InputStream inputStream,
      @Nullable OutputStream outputStream,
      @Nullable OutputStream errorStream)
      throws IOException {
    String[] modifiedArgs = checkConnectionArgs(args);
    TerminalFactory.registerFlavor(TerminalFactory.Flavor.UNIX, InterruptRestoringTerminal.class);
    SqlLine sqlLine = new SqlLine();

    if (outputStream != null) {
      sqlLine.setOutputStream(new PrintStream(outputStream, false, StandardCharsets.UTF_8.name()));
    }

    if (errorStream != null) {
      sqlLine.setErrorStream(new PrintStream(errorStream, false, StandardCharsets.UTF_8.name()));
    }

    return sqlLine.begin(modifiedArgs, inputStream, true);
  }

  /**
   * A JLine {@link UnixTerminal} that always restores the interrupt character (Ctrl+C) after
   * reading a line. SqlLine has JLine handle Ctrl+C while it reads a line, so JLine undefines the
   * character for each read and then restores the value it saved. JLine reads that value from
   * {@code stty} output that it caches for a second, which can be from a read that had already
   * undefined it. Ctrl+C then stops sending SIGINT for good, so it can't cancel a running
   * statement.
   */
  public static class InterruptRestoringTerminal extends UnixTerminal {
    private @Nullable String intr;

    public InterruptRestoringTerminal() throws Exception {}

    @Override
    public void init() throws Exception {
      super.init();
      // No line has been read yet, so the character is still defined.
      String value = getSettings().getPropertyAsString("intr");
      intr = "<undef>".equals(value) ? null : value;
    }

    @Override
    public void disableInterruptCharacter() {
      try {
        getSettings().undef("intr");
      } catch (Exception e) {
        handle(e, "Failed to disable interrupt character");
      }
    }

    @Override
    public void enableInterruptCharacter() {
      String value = intr;
      try {
        if (value != null) {
          getSettings().set("intr", value);
        }
      } catch (Exception e) {
        handle(e, "Failed to enable interrupt character");
      }
    }

    private static void handle(Exception e, String message) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      Log.error(message, e);
    }
  }
}
