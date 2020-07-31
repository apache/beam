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
package org.apache.beam.sdk.testing.expansion;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/**
 * An {@link org.apache.beam.runners.core.construction.expansion.ExpansionService} useful for tests.
 */
public class TestExpansionService {

  private static final String TEST_PREFIX_URN = "beam:transforms:xlang:test:prefix";
  private static final String TEST_MULTI_URN = "beam:transforms:xlang:test:multi";
  private static final String TEST_GBK_URN = "beam:transforms:xlang:test:gbk";
  private static final String TEST_CGBK_URN = "beam:transforms:xlang:test:cgbk";
  private static final String TEST_COMGL_URN = "beam:transforms:xlang:test:comgl";
  private static final String TEST_COMPK_URN = "beam:transforms:xlang:test:compk";
  private static final String TEST_FLATTEN_URN = "beam:transforms:xlang:test:flatten";
  private static final String TEST_PARTITION_URN = "beam:transforms:xlang:test:partition";
  private static final String TEST_PARQUET_WRITE_URN = "beam:transforms:xlang:test:parquet_write";

  private static final String TEST_COUNT_URN = "beam:transforms:xlang:count";
  private static final String TEST_FILTER_URN = "beam:transforms:xlang:filter_less_than_eq";
  private static final String TEST_PARQUET_READ_URN = "beam:transforms:xlang:parquet_read";

  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestServiceRegistrar implements ExpansionService.ExpansionServiceRegistrar {
    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      ImmutableMap.Builder<String, ExpansionService.TransformProvider> builder =
          ImmutableMap.builder();
      builder.put(TEST_CGBK_URN, new TestCoGroupByKeyTransformProvider());
      builder.put(TEST_FLATTEN_URN, new TestFlattenTransformProvider());
      return builder.build();
    }

    public static class TestCoGroupByKeyTransformProvider
        implements ExpansionService.TransformProvider<
            KeyedPCollectionTuple<Long>, PCollection<KV<Long, Iterable<String>>>> {
      public static class TestCoGroupByKeyTransform
          extends PTransform<KeyedPCollectionTuple<Long>, PCollection<KV<Long, Iterable<String>>>> {
        @Override
        public PCollection<KV<Long, Iterable<String>>> expand(KeyedPCollectionTuple<Long> input) {
          Set<String> tagSet = ImmutableSet.of("col1", "col2");
          return input
              .apply(CoGroupByKey.create())
              .apply(
                  ParDo.of(
                      new DoFn<KV<Long, CoGbkResult>, KV<Long, Iterable<String>>>() {
                        @ProcessElement
                        public void processElement(
                            @Element KV<Long, CoGbkResult> kv,
                            OutputReceiver<KV<Long, Iterable<String>>> out) {
                          Iterable<String> iter =
                              () ->
                                  tagSet.stream()
                                      .flatMap(
                                          (String t) ->
                                              StreamSupport.stream(
                                                  kv.getValue().<String>getAll(t).spliterator(),
                                                  false))
                                      .iterator();
                          out.output(KV.of(kv.getKey(), iter));
                        }
                      }));
        }
      }

      @Override
      public KeyedPCollectionTuple<Long> createInput(
          Pipeline p, Map<String, PCollection<?>> inputs) {
        KeyedPCollectionTuple inputTuple = KeyedPCollectionTuple.empty(p);
        for (Map.Entry<String, PCollection<?>> entry : inputs.entrySet()) {
          inputTuple = inputTuple.and(new TupleTag(entry.getKey()), entry.getValue());
        }
        return inputTuple;
      }

      @Override
      public PTransform<KeyedPCollectionTuple<Long>, PCollection<KV<Long, Iterable<String>>>>
          getTransform(RunnerApi.FunctionSpec spec) {
        return new TestCoGroupByKeyTransform();
      }
    }

    public static class TestFlattenTransformProvider
        implements ExpansionService.TransformProvider<PCollectionList<Long>, PCollection<Long>> {
      @Override
      public PCollectionList<Long> createInput(Pipeline p, Map<String, PCollection<?>> inputs) {
        PCollectionList<Long> inputList = PCollectionList.empty(p);
        for (PCollection<?> collection : inputs.values()) {
          inputList = inputList.and((PCollection<Long>) collection);
        }
        return inputList;
      }

      @Override
      public PTransform<PCollectionList<Long>, PCollection<Long>> getTransform(
          RunnerApi.FunctionSpec spec) {
        return Flatten.pCollections();
      }
    }
  }

  @AutoService(ExternalTransformRegistrar.class)
  public static class TestTransformRegistrar implements ExternalTransformRegistrar {
    private static String rawSchema =
        "{ \"type\": \"record\", \"name\": \"testrecord\", \"fields\": "
            + "[ {\"name\": \"name\", \"type\": \"string\"} ]}";
    private static Schema schema = new Schema.Parser().parse(rawSchema);

    @Override
    public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
      ImmutableMap.Builder<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> builder =
          ImmutableMap.builder();
      builder.put(TEST_PREFIX_URN, PrefixBuilder.class);
      builder.put(TEST_MULTI_URN, MultiBuilder.class);
      builder.put(TEST_GBK_URN, GBKBuilder.class);
      builder.put(TEST_COMGL_URN, CombineGloballyBuilder.class);
      builder.put(TEST_COMPK_URN, CombinePerKeyBuilder.class);
      builder.put(TEST_COUNT_URN, CountBuilder.class);
      builder.put(TEST_FILTER_URN, FilterBuilder.class);
      builder.put(TEST_PARTITION_URN, PartitionBuilder.class);
      builder.put(TEST_PARQUET_WRITE_URN, ParquetWriteBuilder.class);
      builder.put(TEST_PARQUET_READ_URN, ParquetReadBuilder.class);
      return builder.build();
    }

    public static class EmptyConfiguration {}

    public static class StringConfiguration implements Serializable {
      private String data;

      public void setData(String data) {
        this.data = data;
      }
    }

    public static class PrefixBuilder
        implements ExternalTransformBuilder<
            StringConfiguration, PCollection<? extends String>, PCollection<String>> {
      @Override
      public PTransform<PCollection<? extends String>, PCollection<String>> buildExternal(
          StringConfiguration configuration) {
        return MapElements.into(TypeDescriptors.strings())
            .via((String x) -> configuration.data + x);
      }
    }

    public static class MultiBuilder
        implements ExternalTransformBuilder<
            EmptyConfiguration, PCollectionTuple, PCollectionTuple> {
      public static class TestMultiPTransform
          extends PTransform<PCollectionTuple, PCollectionTuple> {
        @Override
        public PCollectionTuple expand(PCollectionTuple input) {
          PCollectionView<String> sideView = input.<String>get("side").apply(View.asSingleton());
          PCollection<String> main =
              PCollectionList.<String>of(input.get("main1"))
                  .and(input.get("main2"))
                  .apply(Flatten.pCollections())
                  .apply(
                      ParDo.of(
                              new DoFn<String, String>() {
                                @ProcessElement
                                public void processElement(
                                    @Element String x,
                                    OutputReceiver<String> out,
                                    DoFn<String, String>.ProcessContext c) {
                                  out.output(x + c.sideInput(sideView));
                                }
                              })
                          .withSideInputs(sideView));
          PCollection<String> side =
              input
                  .<String>get("side")
                  .apply(MapElements.into(TypeDescriptors.strings()).via((String x) -> x + x));
          return PCollectionTuple.of("main", main).and("side", side);
        }
      }

      @Override
      public PTransform<PCollectionTuple, PCollectionTuple> buildExternal(
          EmptyConfiguration configuration) {
        return new TestMultiPTransform();
      }
    }

    public static class GBKBuilder
        implements ExternalTransformBuilder<
            EmptyConfiguration,
            PCollection<KV<Long, String>>,
            PCollection<KV<Long, Iterable<String>>>> {
      @Override
      public PTransform<PCollection<KV<Long, String>>, PCollection<KV<Long, Iterable<String>>>>
          buildExternal(EmptyConfiguration configuration) {
        return GroupByKey.create();
      }
    }

    public static class CombineGloballyBuilder
        implements ExternalTransformBuilder<
            EmptyConfiguration, PCollection<Long>, PCollection<Long>> {
      @Override
      public PTransform<PCollection<Long>, PCollection<Long>> buildExternal(
          EmptyConfiguration configuration) {
        return Sum.longsGlobally();
      }
    }

    public static class CombinePerKeyBuilder
        implements ExternalTransformBuilder<
            EmptyConfiguration, PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {
      @Override
      public PTransform<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> buildExternal(
          EmptyConfiguration configuration) {
        return Sum.longsPerKey();
      }
    }

    public static class CountBuilder
        implements ExternalTransformBuilder<
            EmptyConfiguration, PCollection<Object>, PCollection<KV<Object, Long>>> {
      @Override
      public PTransform<PCollection<Object>, PCollection<KV<Object, Long>>> buildExternal(
          EmptyConfiguration configuration) {
        return Count.perElement();
      }
    }

    public static class FilterBuilder
        implements ExternalTransformBuilder<
            StringConfiguration, PCollection<String>, PCollection<String>> {
      @Override
      public PTransform<PCollection<String>, PCollection<String>> buildExternal(
          StringConfiguration configuration) {
        return Filter.lessThanEq(configuration.data);
      }
    }

    public static class PartitionBuilder
        implements ExternalTransformBuilder<
            EmptyConfiguration, PCollection<Long>, PCollectionList<Long>> {
      @Override
      public PTransform<PCollection<Long>, PCollectionList<Long>> buildExternal(
          EmptyConfiguration configuration) {
        return Partition.of(2, (Long elem, int numP) -> elem % 2 == 0 ? 0 : 1);
      }
    }

    public static class ParquetWriteBuilder
        implements ExternalTransformBuilder<
            StringConfiguration, PCollection<GenericRecord>, PCollection<String>> {
      @Override
      public PTransform<PCollection<GenericRecord>, PCollection<String>> buildExternal(
          StringConfiguration configuration) {
        return new PTransform<PCollection<GenericRecord>, PCollection<String>>() {
          @Override
          public PCollection<String> expand(PCollection<GenericRecord> input) {
            return input
                .apply(
                    FileIO.<GenericRecord>write()
                        .via(ParquetIO.sink(schema))
                        .to(configuration.data))
                .getPerDestinationOutputFilenames()
                .apply(Values.create());
          }
        };
      }
    }

    public static class ParquetReadBuilder
        implements ExternalTransformBuilder<
            StringConfiguration, PBegin, PCollection<GenericRecord>> {
      @Override
      public PTransform<PBegin, PCollection<GenericRecord>> buildExternal(
          StringConfiguration configuration) {
        return new PTransform<PBegin, PCollection<GenericRecord>>() {
          @Override
          public PCollection<GenericRecord> expand(PBegin input) {
            return input
                .apply(FileIO.match().filepattern(configuration.data))
                .apply(FileIO.readMatches())
                .apply(ParquetIO.readFiles(schema))
                .setCoder(AvroCoder.of(schema));
          }
        };
      }
    }
  }
}
