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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;

/** Split class used by the tracker. */
public class Split implements Serializable, HasDefaultTracker<Split, SplitTracker> {

  private final int from;
  private final int to;

  public Split(int from, int to) {
    checkArgument(from <= to, "Malformed split [%s, %s)", from, to);
    this.from = from;
    this.to = to;
  }

  public int getFrom() {
    return from;
  }

  public int getTo() {
    return to;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Split)) {
      return false;
    }

    Split split = (Split) o;

    if (from != split.from) {
      return false;
    }
    return to == split.to;
  }

  @Override
  public int hashCode() {
    int result = from;
    result = 31 * result + to;
    return result;
  }

  @Override
  public SplitTracker newTracker() {
    return new SplitTracker(this);
  }
}
