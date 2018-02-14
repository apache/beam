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

import static org.apache.beam.runners.core.construction.UrnUtils.validateCommonUrn;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

/** The {@link CoderTranslatorRegistrar} for coders which are shared across languages. */
@AutoService(CoderTranslatorRegistrar.class)
public class ModelCoderRegistrar implements CoderTranslatorRegistrar {
  // The URNs for coders which are shared across languages
  @VisibleForTesting
  static final BiMap<Class<? extends Coder>, String> BEAM_MODEL_CODER_URNS =
      ImmutableBiMap.<Class<? extends Coder>, String>builder()
          .put(ByteArrayCoder.class, validateCommonUrn("beam:coder:bytes:v1"))
          .put(KvCoder.class, validateCommonUrn("beam:coder:kv:v1"))
          .put(VarLongCoder.class, validateCommonUrn("beam:coder:varint:v1"))
          .put(IntervalWindowCoder.class, validateCommonUrn("beam:coder:interval_window:v1"))
          .put(IterableCoder.class, validateCommonUrn("beam:coder:iterable:v1"))
          .put(LengthPrefixCoder.class, validateCommonUrn("beam:coder:length_prefix:v1"))
          .put(GlobalWindow.Coder.class, validateCommonUrn("beam:coder:global_window:v1"))
          .put(FullWindowedValueCoder.class, validateCommonUrn("beam:coder:windowed_value:v1"))
          .build();

  @VisibleForTesting
  static final Map<Class<? extends Coder>, CoderTranslator<? extends Coder>>
      BEAM_MODEL_CODERS =
      ImmutableMap
          .<Class<? extends Coder>, CoderTranslator<? extends Coder>>
              builder()
          .put(ByteArrayCoder.class, CoderTranslators.atomic(ByteArrayCoder.class))
          .put(VarLongCoder.class, CoderTranslators.atomic(VarLongCoder.class))
          .put(IntervalWindowCoder.class, CoderTranslators.atomic(IntervalWindowCoder.class))
          .put(GlobalWindow.Coder.class, CoderTranslators.atomic(GlobalWindow.Coder.class))
          .put(KvCoder.class, CoderTranslators.kv())
          .put(IterableCoder.class, CoderTranslators.iterable())
          .put(LengthPrefixCoder.class, CoderTranslators.lengthPrefix())
          .put(FullWindowedValueCoder.class, CoderTranslators.fullWindowedValue())
          .build();

  @Override
  public Map<Class<? extends Coder>, String> getCoderURNs() {
    return BEAM_MODEL_CODER_URNS;
  }

  @Override
  public Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> getCoderTranslators() {
    return BEAM_MODEL_CODERS;
  }
}
