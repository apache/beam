/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.dataflow.io;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

/**
 * Print to console.
 */
public final class ConsoleIO {

  private ConsoleIO() {
  }

  public static final class Write {

    private Write() {
    }

    public static <T> Unbound<T> from() {
      return new Unbound<>(10);
    }

    public static <T> Unbound<T> from(int num) {
      return new Unbound<>(num);
    }

    public static class Unbound<T> extends PTransform<PCollection<T>, PDone> {

      private final int num;

      Unbound(int num) {
        this.num = num;
      }

      public int getNum() {
        return num;
      }

      @Override
      public PDone apply(PCollection<T> input) {
        return PDone.in(input.getPipeline());
      }
    }
  }
}
