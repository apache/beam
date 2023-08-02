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
package org.apache.beam.runners.dataflow.util;

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.internal.IsmFormat.FooterCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmShardCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.KeyPrefixCoder;
import org.apache.beam.runners.dataflow.util.RandomAccessData.RandomAccessDataCoder;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.BigIntegerCoder;
import org.apache.beam.sdk.coders.BitSetCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.DurationCoder;
import org.apache.beam.sdk.coders.FloatCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutation;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoderV2;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoderV3;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/**
 * The {@link CoderCloudObjectTranslatorRegistrar} containing the default collection of {@link
 * Coder} {@link CloudObjectTranslator Cloud Object Translators}.
 */
@AutoService(CoderCloudObjectTranslatorRegistrar.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class DefaultCoderCloudObjectTranslatorRegistrar
    implements CoderCloudObjectTranslatorRegistrar {
  private static final List<CloudObjectTranslator<? extends Coder>> DEFAULT_TRANSLATORS =
      ImmutableList.of(
          CloudObjectTranslators.globalWindow(),
          CloudObjectTranslators.intervalWindow(),
          CloudObjectTranslators.customWindow(),
          CloudObjectTranslators.bytes(),
          CloudObjectTranslators.varInt(),
          CloudObjectTranslators.lengthPrefix(),
          CloudObjectTranslators.stream(),
          CloudObjectTranslators.pair(),
          CloudObjectTranslators.windowedValue(),
          new AvroCoderCloudObjectTranslator(),
          new SerializableCoderCloudObjectTranslator(),
          new SchemaCoderCloudObjectTranslator(),
          new RowCoderCloudObjectTranslator(),
          CloudObjectTranslators.iterableLike(CollectionCoder.class),
          CloudObjectTranslators.iterableLike(ListCoder.class),
          CloudObjectTranslators.iterableLike(SetCoder.class),
          CloudObjectTranslators.map(),
          CloudObjectTranslators.timestampedValue(),
          CloudObjectTranslators.nullable(),
          CloudObjectTranslators.union(),
          CloudObjectTranslators.coGroupByKeyResult(),
          CloudObjectTranslators.javaSerialized());
  // TODO: ElementAndRestrictionCoder. This is in runners-core, but probably needs to be
  // in core-construction
  @VisibleForTesting
  static final ImmutableSet<Class<? extends Coder>> KNOWN_ATOMIC_CODERS =
      ImmutableSet.of(
          BigDecimalCoder.class,
          BigEndianIntegerCoder.class,
          BigEndianLongCoder.class,
          BigIntegerCoder.class,
          BitSetCoder.class,
          ByteCoder.class,
          DoubleCoder.class,
          DurationCoder.class,
          FloatCoder.class,
          FooterCoder.class,
          InstantCoder.class,
          IsmShardCoder.class,
          KeyPrefixCoder.class,
          RandomAccessDataCoder.class,
          RowMutation.RowMutationCoder.class,
          StringUtf8Coder.class,
          TableDestinationCoderV2.class,
          TableDestinationCoderV3.class,
          TableRowJsonCoder.class,
          TextualIntegerCoder.class,
          VarIntCoder.class,
          VoidCoder.class);
  // TODO: WriteBundlesToFiles.ResultCoder.class);
  // TODO: Atomic, GCPIO Coders:
  //   TableRowInfoCoder.class
  //   PubsubUnboundedSink.OutgoingMessageCoder.class,
  //   PubsubUnboundedSource.PubsubCheckpointCoder.class,

  @Override
  public Map<String, CloudObjectTranslator<? extends Coder>> classNamesToTranslators() {
    ImmutableMap.Builder<String, CloudObjectTranslator<? extends Coder>> nameToTranslators =
        ImmutableMap.builder();
    for (CloudObjectTranslator<? extends Coder> translator : classesToTranslators().values()) {
      nameToTranslators.put(translator.cloudObjectClassName(), translator);
    }
    return nameToTranslators.build();
  }

  @Override
  public Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      classesToTranslators() {
    ImmutableMap.Builder<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> builder =
        ImmutableMap.builder();
    for (CloudObjectTranslator<? extends Coder> defaultTranslator : DEFAULT_TRANSLATORS) {
      builder.put(defaultTranslator.getSupportedClass(), defaultTranslator);
    }
    for (Class<? extends Coder> atomicCoder : KNOWN_ATOMIC_CODERS) {
      builder.put(atomicCoder, CloudObjectTranslators.atomic(atomicCoder));
    }
    return builder.build();
  }
}
