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
package org.apache.beam.sdk.schemas.io;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

@AutoService(GenericDlqProvider.class)
public class StoringDlqProvider implements GenericDlqProvider {
  static final String ID = "storing_dlq_provider_testonly_do_not_use";
  static final String CONFIG = "storing_dlq_provider_required_config_value";
  private static final List<Failure> FAILURES = new ArrayList<>();

  @Override
  public String identifier() {
    return ID;
  }

  public static synchronized void reset() {
    FAILURES.clear();
  }

  private static synchronized void addValue(Failure failure) {
    FAILURES.add(failure);
  }

  public static synchronized List<Failure> getFailures() {
    return ImmutableList.copyOf(FAILURES);
  }

  @Override
  public PTransform<PCollection<Failure>, PDone> newDlqTransform(String config) {
    checkArgument(config.equals(CONFIG));
    return new PTransform<PCollection<Failure>, PDone>() {
      @Override
      public PDone expand(PCollection<Failure> input) {
        input.apply(
            MapElements.into(TypeDescriptor.of(Void.class))
                .via(
                    x -> {
                      addValue(x);
                      return null;
                    }));
        return PDone.in(input.getPipeline());
      }
    };
  }
}
