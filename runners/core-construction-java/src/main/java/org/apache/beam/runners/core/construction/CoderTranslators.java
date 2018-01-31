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

package org.apache.beam.runners.core.construction;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

/** {@link CoderTranslator} implementations for known coder types. */
class CoderTranslators {
  private CoderTranslators() {}

  static <T extends Coder<?>> CoderTranslator<T> atomic(final Class<T> clazz) {
    return new SimpleStructuredCoderTranslator<T>() {
      @Override
      public List<? extends Coder<?>> getComponents(T from) {
        return Collections.emptyList();
      }

      @Override
      public T fromComponents(List<Coder<?>> components) {
        return InstanceBuilder.ofType(clazz).build();
      }
    };
  }

  static CoderTranslator<KvCoder<?, ?>> kv() {
    return new SimpleStructuredCoderTranslator<KvCoder<?, ?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(KvCoder<?, ?> from) {
        return ImmutableList.of(from.getKeyCoder(), from.getValueCoder());
      }

      @Override
      public KvCoder<?, ?> fromComponents(List<Coder<?>> components) {
        return KvCoder.of(components.get(0), components.get(1));
      }
    };
  }

  static CoderTranslator<IterableCoder<?>> iterable() {
    return new SimpleStructuredCoderTranslator<IterableCoder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(IterableCoder<?> from) {
        return Collections.singletonList(from.getElemCoder());
      }

      @Override
      public IterableCoder<?> fromComponents(List<Coder<?>> components) {
        return IterableCoder.of(components.get(0));
      }
    };
  }

  static CoderTranslator<LengthPrefixCoder<?>> lengthPrefix() {
    return new SimpleStructuredCoderTranslator<LengthPrefixCoder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(LengthPrefixCoder<?> from) {
        return Collections.singletonList(from.getValueCoder());
      }

      @Override
      public LengthPrefixCoder<?> fromComponents(List<Coder<?>> components) {
        return LengthPrefixCoder.of(components.get(0));
      }
    };
  }

  static CoderTranslator<FullWindowedValueCoder<?>> fullWindowedValue() {
    return new SimpleStructuredCoderTranslator<FullWindowedValueCoder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(FullWindowedValueCoder<?> from) {
        return ImmutableList.of(from.getValueCoder(), from.getWindowCoder());
      }

      @Override
      public FullWindowedValueCoder<?> fromComponents(List<Coder<?>> components) {
        return WindowedValue.getFullCoder(
            components.get(0), (Coder<BoundedWindow>) components.get(1));
      }
    };
  }

  public abstract static class SimpleStructuredCoderTranslator<T extends Coder<?>>
      implements CoderTranslator<T> {
    public final T fromComponents(List<Coder<?>> components, byte[] payload) {
      return fromComponents(components);
    }

    protected abstract T fromComponents(List<Coder<?>> components);
  }
}
