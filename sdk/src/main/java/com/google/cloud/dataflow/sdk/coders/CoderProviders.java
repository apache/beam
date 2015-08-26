/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * A {@code CoderProvider} may create a {@link Coder} for
 * any concrete class.
 */
public final class CoderProviders {

  // Static utility class
  private CoderProviders() { }

  /**
   * Returns a {@link CoderProvider} that consults each of the provider {@code coderProviders}
   * and returns the first {@link Coder} provided.
   *
   * <p>Note that while the number of types handled will be the union of those handled by all of
   * the provided {@code coderProviders}, the actual {@link Coder} provided by an earlier provider
   * may have inferior determinism properties.
   */
  public static CoderProvider firstOf(CoderProvider... coderProviders) {
    return new FirstOf(ImmutableList.copyOf(coderProviders));
  }

  /**
   * @see #firstOf
   */
  private static class FirstOf implements CoderProvider {

    private Iterable<CoderProvider> providers;

    public FirstOf(Iterable<CoderProvider> providers) {
      this.providers = providers;
    }

    @Override
    public <T> Coder<T> getCoder(TypeDescriptor<T> type) throws CannotProvideCoderException {
      List<String> messages = Lists.newArrayList();
      for (CoderProvider provider : providers) {
        try {
          return provider.getCoder(type);
        } catch (CannotProvideCoderException exc) {
          messages.add(String.format("%s could not provide a Coder for type %s: %s",
              provider, type, exc.getMessage()));
        }
      }
      throw new CannotProvideCoderException(
          String.format("Cannot provide coder for type %s: %s.",
              type, Joiner.on("; ").join(messages)));
    }
  }
}
