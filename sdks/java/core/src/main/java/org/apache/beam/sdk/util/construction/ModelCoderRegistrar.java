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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TimestampPrefixingWindowCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

/** The {@link CoderTranslatorRegistrar} for coders which are shared across languages. */
@AutoService(CoderTranslatorRegistrar.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness",
  "keyfor"
}) // TODO(https://github.com/apache/beam/issues/20497)
public class ModelCoderRegistrar implements CoderTranslatorRegistrar {

  // The URNs for coders which are shared across languages
  private static final BiMap<Class<? extends Coder>, String> BEAM_MODEL_CODER_URNS =
      ImmutableBiMap.<Class<? extends Coder>, String>builder()
          .put(ByteArrayCoder.class, ModelCoders.BYTES_CODER_URN)
          .put(BooleanCoder.class, ModelCoders.BOOL_CODER_URN)
          .put(StringUtf8Coder.class, ModelCoders.STRING_UTF8_CODER_URN)
          .put(KvCoder.class, ModelCoders.KV_CODER_URN)
          .put(VarLongCoder.class, ModelCoders.INT64_CODER_URN)
          .put(IntervalWindowCoder.class, ModelCoders.INTERVAL_WINDOW_CODER_URN)
          .put(IterableCoder.class, ModelCoders.ITERABLE_CODER_URN)
          .put(Timer.Coder.class, ModelCoders.TIMER_CODER_URN)
          .put(LengthPrefixCoder.class, ModelCoders.LENGTH_PREFIX_CODER_URN)
          .put(GlobalWindow.Coder.class, ModelCoders.GLOBAL_WINDOW_CODER_URN)
          .put(FullWindowedValueCoder.class, ModelCoders.WINDOWED_VALUE_CODER_URN)
          .put(
              WindowedValue.ParamWindowedValueCoder.class,
              ModelCoders.PARAM_WINDOWED_VALUE_CODER_URN)
          .put(DoubleCoder.class, ModelCoders.DOUBLE_CODER_URN)
          .put(RowCoder.class, ModelCoders.ROW_CODER_URN)
          .put(ShardedKey.Coder.class, ModelCoders.SHARDED_KEY_CODER_URN)
          .put(TimestampPrefixingWindowCoder.class, ModelCoders.CUSTOM_WINDOW_CODER_URN)
          .put(NullableCoder.class, ModelCoders.NULLABLE_CODER_URN)
          .build();

  private static final Map<Class<? extends Coder>, CoderTranslator<? extends Coder>>
      BEAM_MODEL_CODERS =
          ImmutableMap.<Class<? extends Coder>, CoderTranslator<? extends Coder>>builder()
              .put(ByteArrayCoder.class, CoderTranslators.atomic(ByteArrayCoder.class))
              .put(BooleanCoder.class, CoderTranslators.atomic(BooleanCoder.class))
              .put(StringUtf8Coder.class, CoderTranslators.atomic(StringUtf8Coder.class))
              .put(VarLongCoder.class, CoderTranslators.atomic(VarLongCoder.class))
              .put(IntervalWindowCoder.class, CoderTranslators.atomic(IntervalWindowCoder.class))
              .put(GlobalWindow.Coder.class, CoderTranslators.atomic(GlobalWindow.Coder.class))
              .put(KvCoder.class, CoderTranslators.kv())
              .put(IterableCoder.class, CoderTranslators.iterable())
              .put(Timer.Coder.class, CoderTranslators.timer())
              .put(LengthPrefixCoder.class, CoderTranslators.lengthPrefix())
              .put(FullWindowedValueCoder.class, CoderTranslators.fullWindowedValue())
              .put(
                  WindowedValue.ParamWindowedValueCoder.class,
                  CoderTranslators.paramWindowedValue())
              .put(DoubleCoder.class, CoderTranslators.atomic(DoubleCoder.class))
              .put(RowCoder.class, CoderTranslators.row())
              .put(ShardedKey.Coder.class, CoderTranslators.shardedKey())
              .put(TimestampPrefixingWindowCoder.class, CoderTranslators.timestampPrefixingWindow())
              .put(NullableCoder.class, CoderTranslators.nullable())
              .build();

  static {
    checkState(
        BEAM_MODEL_CODERS.keySet().containsAll(BEAM_MODEL_CODER_URNS.keySet()),
        "Every Model %s must have an associated %s. Missing: %s",
        Coder.class.getSimpleName(),
        CoderTranslator.class.getSimpleName(),
        Sets.difference(BEAM_MODEL_CODER_URNS.keySet(), BEAM_MODEL_CODERS.keySet()));
    checkState(
        Sets.symmetricDifference(
                ModelCoders.urns(),
                /**
                 * The state backed iterable coder implementation is environment specific and hence
                 * is not part of the coder translation checks as these are meant to be used only
                 * during pipeline construction.
                 */
                Collections.singleton(ModelCoders.STATE_BACKED_ITERABLE_CODER_URN))
            .equals(BEAM_MODEL_CODER_URNS.values()),
        "All Model %ss should have an associated java %s",
        Coder.class.getSimpleName(),
        Coder.class.getSimpleName());
  }

  public static boolean isKnownCoder(Coder<?> coder) {
    return BEAM_MODEL_CODER_URNS.containsKey(coder.getClass());
  }

  @Override
  public Map<Class<? extends Coder>, String> getCoderURNs() {
    return BEAM_MODEL_CODER_URNS;
  }

  @Override
  public Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> getCoderTranslators() {
    return BEAM_MODEL_CODERS;
  }
}
