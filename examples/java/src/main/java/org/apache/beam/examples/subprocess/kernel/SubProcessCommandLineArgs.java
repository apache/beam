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
package org.apache.beam.examples.subprocess.kernel;

import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/** Parameters to the sub-process, has tuple of ordinal position and the value. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SubProcessCommandLineArgs {

  // Parameters to pass to the sub-process
  private List<Command> parameters = Lists.newArrayList();

  public void addCommand(Integer position, String value) {
    parameters.add(new Command(position, value));
  }

  public void putCommand(Command command) {
    parameters.add(command);
  }

  public List<Command> getParameters() {
    return parameters;
  }

  /** Class used to store the SubProcces parameters. */
  public static class Command {

    // The ordinal position of the command to pass to the sub-process
    int ordinalPosition;
    String value;

    @SuppressWarnings("unused")
    private Command() {}

    public Command(int ordinalPosition, String value) {
      this.ordinalPosition = ordinalPosition;
      this.value = value;
    }

    public int getKey() {
      return ordinalPosition;
    }

    public void setKey(int key) {
      this.ordinalPosition = key;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }
}
