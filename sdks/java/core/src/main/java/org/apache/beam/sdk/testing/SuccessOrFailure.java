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
package org.apache.beam.sdk.testing;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.util.SerializableThrowable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Output of {@link PAssert}. Passed to a conclude function to act upon. */
@DefaultCoder(SerializableCoder.class)
public final class SuccessOrFailure implements Serializable {

  private final boolean isSuccess;
  private final PAssert.@Nullable PAssertionSite site;
  private final @Nullable SerializableThrowable throwable;

  private SuccessOrFailure(
      boolean isSuccess, PAssert.@Nullable PAssertionSite site, @Nullable Throwable throwable) {
    this.isSuccess = isSuccess;
    this.site = site;
    this.throwable = new SerializableThrowable(throwable);
  }

  public boolean isSuccess() {
    return isSuccess;
  }

  public @Nullable AssertionError assertionError() {
    return site == null ? null : site.wrap(throwable.getThrowable());
  }

  public static SuccessOrFailure success() {
    return new SuccessOrFailure(true, null, null);
  }

  public static SuccessOrFailure failure(
      PAssert.@Nullable PAssertionSite site, @Nullable Throwable t) {
    return new SuccessOrFailure(false, site, t);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("isSuccess", isSuccess())
        .addValue(throwable)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SuccessOrFailure that = (SuccessOrFailure) o;
    return isSuccess == that.isSuccess
        && Objects.equal(site, that.site)
        && Objects.equal(throwable, that.throwable);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(isSuccess, site, throwable);
  }
}
