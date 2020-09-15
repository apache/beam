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
package org.apache.beam.sdk.transforms.splittabledofn;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A representation of a split result. */
@Experimental(Kind.SPLITTABLE_DO_FN)
@AutoValue
public abstract class SplitResult<RestrictionT> {
  /** Returns a {@link SplitResult} for the specified primary and residual restrictions. */
  public static <RestrictionT> SplitResult<RestrictionT> of(
      RestrictionT primary, RestrictionT residual) {
    return new AutoValue_SplitResult(primary, residual);
  }

  /** Returns the primary restriction. */
  public abstract @Nullable RestrictionT getPrimary();

  /** Returns the residual restriction. */
  public abstract @Nullable RestrictionT getResidual();
}
