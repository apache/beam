/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

/**
 * Some simple functions (commutative and associative with a zero)
 * usable with {@link MovingFunction} and {@link BucketingFunction}.
 */
public enum SimpleFunction {
  MIN {
    @Override
    public long f(long l, long r) {
      return Math.min(l, r);
    }

    @Override
    public long zero() {
      return Long.MAX_VALUE;
    }
  },
  MAX {
    @Override
    public long f(long l, long r) {
      return Math.max(l, r);
    }

    @Override
    public long zero() {
      return Long.MIN_VALUE;
    }
  },
  SUM {
    @Override
    public long f(long l, long r) {
      return l + r;
    }

    @Override
    public long zero() {
      return 0;
    }
  };

  public abstract long f(long l, long r);

  public abstract long zero();
}

