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
package org.apache.beam.examples.multilanguage;

import java.io.Serializable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

public class JavaDataGenerator extends PTransform<PBegin, PCollection<String>> {
  private Integer size;
  private @NonNull JavaDataGeneratorConfig dataConfig;

  public JavaDataGenerator(Integer size) {
    this.size = size;
    dataConfig = new JavaDataGeneratorConfig("", 10, "");
  }

  public JavaDataGenerator(Integer size, @NonNull JavaDataGeneratorConfig dataConfig) {
    this(size);
    this.dataConfig = dataConfig;
  }

  public static JavaDataGenerator create(Integer size) {
    return new JavaDataGenerator(size);
  }

  static class JavaDataGeneratorConfig implements Serializable {
    public @NonNull String prefix;
    public long length;
    public @NonNull String suffix;

    public JavaDataGeneratorConfig(@NonNull String prefix, long length, @NonNull String suffix) {
      this.prefix = prefix;
      this.length = length;
      this.suffix = suffix;
    }

    public JavaDataGeneratorConfig() {
      this.prefix = "";
      this.length = 10;
      this.suffix = "";
    }
  }

  public JavaDataGenerator withDataConfig(JavaDataGeneratorConfig dataConfig) {
    return new JavaDataGenerator(this.size, dataConfig);
  }

  static class GenerateDataDoFn extends DoFn<Long, String> {

    private @NonNull JavaDataGeneratorConfig dataConfig;

    public GenerateDataDoFn(@NonNull JavaDataGeneratorConfig dataConfig) {
      if (dataConfig == null) {
        dataConfig = new JavaDataGeneratorConfig("", 100, "");
      }
      this.dataConfig = dataConfig;
    }

    @ProcessElement
    public void process(@Element Long input, OutputReceiver<String> o) {
      String prefix = this.dataConfig.prefix != null ? this.dataConfig.prefix : "";
      String suffix = this.dataConfig.suffix != null ? this.dataConfig.suffix : "";
      int remainingLength =
          Math.max(0, (int) this.dataConfig.length - prefix.length() - suffix.length());
      o.output(prefix + StringUtils.repeat("*", remainingLength) + suffix);
    }
  }

  @Override
  public PCollection<String> expand(PBegin input) {
    return input
        .apply(GenerateSequence.from(0).to(this.size))
        .apply(ParDo.of(new GenerateDataDoFn(this.dataConfig)));
  }
}
