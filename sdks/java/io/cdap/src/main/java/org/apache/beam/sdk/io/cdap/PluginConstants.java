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
package org.apache.beam.sdk.io.cdap;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

/** Class for CDAP plugin constants. */
public final class PluginConstants {
  /** Plugin types. */
  public enum PluginType {
    SOURCE,
    SINK
  }

  /** Format types. */
  public enum Format {
    INPUT(InputFormat.class, "InputFormat"),
    OUTPUT(OutputFormat.class, "OutputFormat");

    private final Class<?> formatClass;
    private final String formatName;

    Format(Class<?> formatClass, String formatName) {
      this.formatClass = formatClass;
      this.formatName = formatName;
    }

    public Class<?> getFormatClass() {
      return formatClass;
    }

    public String getFormatName() {
      return formatName;
    }
  }

  /** Format provider types. */
  public enum FormatProvider {
    INPUT(InputFormatProvider.class, "InputFormatProvider"),
    OUTPUT(OutputFormatProvider.class, "OutputFormatProvider");

    private final Class<?> formatProviderClass;
    private final String formatProviderName;

    FormatProvider(Class<?> formatProviderClass, String formatProviderName) {
      this.formatProviderClass = formatProviderClass;
      this.formatProviderName = formatProviderName;
    }

    public Class<?> getFormatProviderClass() {
      return formatProviderClass;
    }

    public String getFormatProviderName() {
      return formatProviderName;
    }
  }

  /** Hadoop types. */
  public enum Hadoop {
    SOURCE("mapreduce.job.inputformat.class", "key.class", "value.class"),
    SINK(
        "mapreduce.job.outputformat.class",
        "mapreduce.job.output.key.class",
        "mapreduce.job.output.value.class");

    private final String formatClass;
    private final String keyClass;
    private final String valueClass;

    Hadoop(String formatClassName, String keyClass, String valueClass) {
      this.formatClass = formatClassName;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }

    public String getFormatClass() {
      return formatClass;
    }

    public String getKeyClass() {
      return keyClass;
    }

    public String getValueClass() {
      return valueClass;
    }
  }
}
