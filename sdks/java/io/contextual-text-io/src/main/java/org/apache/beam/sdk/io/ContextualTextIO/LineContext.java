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
package org.apache.beam.sdk.io.ContextualTextIO;

import com.google.auto.value.AutoValue;
import java.io.Serializable;

@AutoValue
public abstract class LineContext implements Serializable {
  public abstract Long getRangeLineNum();

  public abstract Long getLineNum();

  public abstract Long getRangeNum();

  public abstract String getLine();

  public abstract Builder toBuilder();

  public abstract String getFile();

  public static Builder newBuilder() {
    return new AutoValue_LineContext.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRangeLineNum(Long lineNum);

    public abstract Builder setLineNum(Long lineNum);

    public abstract Builder setRangeNum(Long rangeNum);

    public abstract Builder setLine(String line);

    public abstract Builder setFile(String file);

    public abstract LineContext build();
  }
}
