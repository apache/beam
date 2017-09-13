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
package org.apache.beam.runners.spark.io;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/** Print to console. */
public final class ConsoleIO {

  private ConsoleIO() {}

  /** Write to console. */
  public static final class Write {

    private static final int DEFAULT_NUM = 10;

    private Write() {}

    /** Prints {@value #DEFAULT_NUM} elements from the {@link PCollection} to the console. */
    public static <T> Unbound<T> out() {
      return new Unbound<>(DEFAULT_NUM);
    }

    /** Prints {@code num} elements from the {@link PCollection} to stdout. */
    public static <T> Unbound<T> out(int num) {
      return new Unbound<>(num);
    }

    /**
     * {@link PTransform} writing {@link PCollection} to the console.
     *
     * @param <T> the type of the elements in the {@link PCollection}
     */
    public static class Unbound<T> extends PTransform<PCollection<T>, PDone> {

      private final int num;

      Unbound(int num) {
        this.num = num;
      }

      public int getNum() {
        return num;
      }

      @Override
      public PDone expand(PCollection<T> input) {
        return PDone.in(input.getPipeline());
      }
    }
  }
}
