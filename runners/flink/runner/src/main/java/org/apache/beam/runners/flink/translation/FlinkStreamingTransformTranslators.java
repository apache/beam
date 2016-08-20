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

package org.apache.beam.runners.flink.translation;

import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetDoFn;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.FlinkCoder;
import org.apache.beam.runners.flink.translation.wrappers.SourceInputFormat;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.FlinkStreamingCreateFunction;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedFlinkSink;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedFlinkSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.SystemReduceFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

import com.google.api.client.util.Maps;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class contains all the mappings between Beam and Flink
 * <b>streaming</b> transformations. The {@link FlinkStreamingPipelineTranslator}
 * traverses the Beam job and comes here to translate the encountered Beam transformations
 * into Flink one, based on the mapping available in this class.
 */
public class FlinkStreamingTransformTranslators {

  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private static final Map<Class<? extends PTransform>, FlinkStreamingPipelineTranslator.StreamTransformTranslator> TRANSLATORS = new HashMap<>();

  // here you can find all the available translators.
  static {
    TRANSLATORS.put(Create.Values.class, new CreateStreamingTranslator());
    TRANSLATORS.put(Read.Bounded.class, new BoundedReadSourceTranslator());
    TRANSLATORS.put(Read.Unbounded.class, new UnboundedReadSourceTranslator());
    TRANSLATORS.put(ParDo.Bound.class, new ParDoBoundStreamingTranslator());
    TRANSLATORS.put(TextIO.Write.Bound.class, new TextIOWriteBoundStreamingTranslator());

    TRANSLATORS.put(Write.Bound.class, new WriteSinkStreamingTranslator());

    TRANSLATORS.put(Window.Bound.class, new WindowBoundTranslator());
    TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslator());
    TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslator());
    TRANSLATORS.put(Flatten.FlattenPCollectionList.class, new FlattenPCollectionTranslator());
    TRANSLATORS.put(ParDo.BoundMulti.class, new ParDoBoundMultiStreamingTranslator());
  }

  public static FlinkStreamingPipelineTranslator.StreamTransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
    return TRANSLATORS.get(transform.getClass());
  }

  // --------------------------------------------------------------------------------------------
  //  Transformation Implementations
  // --------------------------------------------------------------------------------------------

  private static class CreateStreamingTranslator<OUT> implements
      FlinkStreamingPipelineTranslator.StreamTransformTranslator<Create.Values<OUT>> {

    @Override
    public void translateNode(Create.Values<OUT> transform, FlinkStreamingTranslationContext context) {
      PCollection<OUT> output = context.getOutput(transform);
      Iterable<OUT> elements = transform.getElements();

      // we need to serialize the elements to byte arrays, since they might contain
      // elements that are not serializable by Java serialization. We deserialize them
      // in the FlatMap function using the Coder.

      List<byte[]> serializedElements = Lists.newArrayList();
      Coder<OUT> elementCoder = output.getCoder();
      for (OUT element: elements) {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        try {
          elementCoder.encode(element, bao, Coder.Context.OUTER);
          serializedElements.add(bao.toByteArray());
        } catch (IOException e) {
          throw new RuntimeException("Could not serialize Create elements using Coder: " + e);
        }
      }


      DataStream<Integer> initDataSet = context.getExecutionEnvironment().fromElements(1);

      FlinkStreamingCreateFunction<Integer, OUT> createFunction =
          new FlinkStreamingCreateFunction<>(serializedElements, elementCoder);

      WindowedValue.ValueOnlyWindowedValueCoder<OUT> windowCoder = WindowedValue.getValueOnlyCoder(elementCoder);
      TypeInformation<WindowedValue<OUT>> outputType = new CoderTypeInformation<>(windowCoder);

      DataStream<WindowedValue<OUT>> outputDataStream = initDataSet.flatMap(createFunction)
          .returns(outputType);

      context.setOutputDataStream(output, outputDataStream);
    }
  }


  private static class TextIOWriteBoundStreamingTranslator<T> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<TextIO.Write.Bound<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(TextIOWriteBoundStreamingTranslator.class);

    @Override
    public void translateNode(TextIO.Write.Bound<T> transform, FlinkStreamingTranslationContext context) {
      PValue input = context.getInput(transform);
      DataStream<WindowedValue<T>> inputDataStream = context.getInputDataStream(input);

      String filenamePrefix = transform.getFilenamePrefix();
      String filenameSuffix = transform.getFilenameSuffix();
      boolean needsValidation = transform.needsValidation();
      int numShards = transform.getNumShards();
      String shardNameTemplate = transform.getShardNameTemplate();

      // TODO: Implement these. We need Flink support for this.
      LOG.warn("Translation of TextIO.Write.needsValidation not yet supported. Is: {}.", needsValidation);
      LOG.warn("Translation of TextIO.Write.filenameSuffix not yet supported. Is: {}.", filenameSuffix);
      LOG.warn("Translation of TextIO.Write.shardNameTemplate not yet supported. Is: {}.", shardNameTemplate);

      DataStream<String> dataSink = inputDataStream.flatMap(new FlatMapFunction<WindowedValue<T>, String>() {
        @Override
        public void flatMap(WindowedValue<T> value, Collector<String> out) throws Exception {
          out.collect(value.getValue().toString());
        }
      });
      DataStreamSink<String> output = dataSink.writeAsText(filenamePrefix, FileSystem.WriteMode.OVERWRITE);

      if (numShards > 0) {
        output.setParallelism(numShards);
      }
    }
  }

  private static class WriteSinkStreamingTranslator<T> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<Write.Bound<T>> {

    @Override
    public void translateNode(Write.Bound<T> transform, FlinkStreamingTranslationContext context) {
      String name = transform.getName();
      PValue input = context.getInput(transform);

      Sink<T> sink = transform.getSink();
      if (!(sink instanceof UnboundedFlinkSink)) {
        throw new UnsupportedOperationException("At the time, only unbounded Flink sinks are supported.");
      }

      DataStream<WindowedValue<T>> inputDataSet = context.getInputDataStream(input);

      inputDataSet.flatMap(new FlatMapFunction<WindowedValue<T>, Object>() {
        @Override
        public void flatMap(WindowedValue<T> value, Collector<Object> out) throws Exception {
          out.collect(value.getValue());
        }
      }).addSink(((UnboundedFlinkSink<Object>) sink).getFlinkSource()).name(name);
    }
  }

  private static class BoundedReadSourceTranslator<T>
      implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<Read.Bounded<T>> {

    @Override
    public void translateNode(Read.Bounded<T> transform, FlinkStreamingTranslationContext context) {

      BoundedSource<T> boundedSource = transform.getSource();
      PCollection<T> output = context.getOutput(transform);

      TypeInformation<WindowedValue<T>> typeInfo = context.getTypeInfo(output);

      DataStream<WindowedValue<T>> source = context.getExecutionEnvironment().createInput(
          new SourceInputFormat<>(
              boundedSource,
              context.getPipelineOptions()),
          typeInfo);

      context.setOutputDataStream(output, source);
    }
  }

  private static class UnboundedReadSourceTranslator<T> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<Read.Unbounded<T>> {

    @Override
    public void translateNode(Read.Unbounded<T> transform, FlinkStreamingTranslationContext context) {
      PCollection<T> output = context.getOutput(transform);

      DataStream<WindowedValue<T>> source;
      if (transform.getSource().getClass().equals(UnboundedFlinkSource.class)) {
        @SuppressWarnings("unchecked")
        UnboundedFlinkSource<T> flinkSourceFunction = (UnboundedFlinkSource<T>) transform.getSource();
        final AssignerWithPeriodicWatermarks<T> flinkAssigner = flinkSourceFunction.getFlinkTimestampAssigner();

        DataStream<T> flinkSource = context.getExecutionEnvironment()
            .addSource(flinkSourceFunction.getFlinkSource());

        flinkSourceFunction.setCoder(
            new FlinkCoder<T>(flinkSource.getType(),
              context.getExecutionEnvironment().getConfig()));

        source = flinkSource
            .assignTimestampsAndWatermarks(flinkAssigner)
            .flatMap(new FlatMapFunction<T, WindowedValue<T>>() {
              @Override
              public void flatMap(T s, Collector<WindowedValue<T>> collector) throws Exception {
                collector.collect(
                    WindowedValue.of(
                        s,
                        new Instant(flinkAssigner.extractTimestamp(s, -1)),
                        GlobalWindow.INSTANCE,
                        PaneInfo.NO_FIRING));
              }});
      } else {
        try {
          transform.getSource();
          UnboundedSourceWrapper<T, ?> sourceWrapper =
              new UnboundedSourceWrapper<>(
                  context.getPipelineOptions(),
                  transform.getSource(),
                  context.getExecutionEnvironment().getParallelism());
          source = context.getExecutionEnvironment().addSource(sourceWrapper).name(transform.getName());
        } catch (Exception e) {
          throw new RuntimeException("Error while translating UnboundedSource: " + transform.getSource(), e);
        }
      }

      context.setOutputDataStream(output, source);
    }
  }

  private static class ParDoBoundStreamingTranslator<IN, OUT> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<ParDo.Bound<IN, OUT>> {

    @Override
    public void translateNode(ParDo.Bound<IN, OUT> transform, FlinkStreamingTranslationContext context) {

      WindowingStrategy<?, ?> windowingStrategy = context.getOutput(transform).getWindowingStrategy();

      TypeInformation<WindowedValue<OUT>> typeInfo = context.getTypeInfo(context.getOutput(transform));

      DoFnOperator<IN, OUT, WindowedValue<OUT>> doFnOperator = new DoFnOperator<>(
          transform.getFn(),
          new TupleTag<OUT>("main output"),
          Collections.<TupleTag<?>>emptyList(),
          new DoFnOperator.DefaultOutputManagerFactory<WindowedValue<OUT>>(),
          windowingStrategy,
          new HashMap<PCollectionView<?>, WindowingStrategy<?, ?>>(),
          context.getPipelineOptions());

      DataStream<WindowedValue<IN>> inputDataStream = context.getInputDataStream(context.getInput(transform));
      SingleOutputStreamOperator<WindowedValue<OUT>> outDataStream = inputDataStream
          .transform(transform.getName(), typeInfo, doFnOperator);

      context.setOutputDataStream(context.getOutput(transform), outDataStream);
    }
  }

  public static class WindowBoundTranslator<T>
      implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<Window.Bound<T>> {

    @Override
    public void translateNode(
        Window.Bound<T> transform,
        FlinkStreamingTranslationContext context) {

      @SuppressWarnings("unchecked")
      WindowingStrategy<T, BoundedWindow> windowingStrategy =
          (WindowingStrategy<T, BoundedWindow>) context.getOutput(transform).getWindowingStrategy();

      TypeInformation<WindowedValue<T>> typeInfo =
          context.getTypeInfo(context.getOutput(transform));

      OldDoFn<T, T> windowAssignerDoFn =
          createWindowAssigner(windowingStrategy.getWindowFn());

      DoFnOperator<T, T, WindowedValue<T>> doFnOperator = new DoFnOperator<>(
          windowAssignerDoFn,
          new TupleTag<T>("main output"),
          Collections.<TupleTag<?>>emptyList(),
          new DoFnOperator.DefaultOutputManagerFactory<WindowedValue<T>>(),
          windowingStrategy,
          new HashMap<PCollectionView<?>, WindowingStrategy<?, ?>>(),
          context.getPipelineOptions());

      DataStream<WindowedValue<T>> inputDataStream =
          context.getInputDataStream(context.getInput(transform));
      SingleOutputStreamOperator<WindowedValue<T>> outDataStream = inputDataStream
          .transform(transform.getName(), typeInfo, doFnOperator);

      context.setOutputDataStream(context.getOutput(transform), outDataStream);
    }

    private static <T, W extends BoundedWindow> OldDoFn<T, T> createWindowAssigner(
        final WindowFn<T, W> windowFn) {

      return new OldDoFn<T, T>() {

        @Override
        public void processElement(final ProcessContext c) throws Exception {
          Collection<W> windows = windowFn.assignWindows(
              windowFn.new AssignContext() {
                @Override
                public T element() {
                  return c.element();
                }

                @Override
                public Instant timestamp() {
                  return c.timestamp();
                }

                @Override
                public BoundedWindow window() {
                  return Iterables.getOnlyElement(c.windowingInternals().windows());
                }
              });

          c.windowingInternals().outputWindowedValue(
              c.element(), c.timestamp(), windows, c.pane());
        }
      };
    }
  }

  public static class GroupByKeyTranslator<K, InputT>
      implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<GroupByKey<K, InputT>> {

    @Override
    public void translateNode(GroupByKey<K, InputT> transform, FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder = SingletonKeyedWorkItemCoder.of(
          inputKvCoder.getKeyCoder(),
          inputKvCoder.getValueCoder(),
          input.getWindowingStrategy().getWindowFn().windowCoder());

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);


      WindowedValue.ValueOnlyWindowedValueCoder<
          SingletonKeyedWorkItem<K, InputT>> windowedWorkItemCoder =
          WindowedValue.getValueOnlyCoder(workItemCoder);

      CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder);

      DataStream<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new CombinePerKeyTranslator.ToKeyedWorkItem<K, InputT>())
              .returns(workItemTypeInfo).name("ToKeyedWorkItem");

      KeyedStream<
          WindowedValue<
              SingletonKeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream = workItemStream
          .keyBy(new WorkItemKeySelector<K, InputT>(inputKvCoder.getKeyCoder()));

      SystemReduceFn<K, InputT, Iterable<InputT>, Iterable<InputT>, BoundedWindow> reduceFn =
          SystemReduceFn.buffering(inputKvCoder.getValueCoder());

      TypeInformation<WindowedValue<KV<K, Iterable<InputT>>>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      DoFnOperator.DefaultOutputManagerFactory<
            WindowedValue<KV<K, Iterable<InputT>>>> outputManagerFactory =
          new DoFnOperator.DefaultOutputManagerFactory<>();

      WindowDoFnOperator<
            K,
            InputT,
            KV<K, Iterable<InputT>>> doFnOperator =
          new WindowDoFnOperator<>(
              reduceFn,
              new TupleTag<KV<K, Iterable<InputT>>>("main output"),
              Collections.<TupleTag<?>>emptyList(),
              outputManagerFactory,
              windowingStrategy,
              new HashMap<PCollectionView<?>, WindowingStrategy<?, ?>>(),
              context.getPipelineOptions(),
              inputKvCoder.getKeyCoder());

      // our operator excepts WindowedValue<KeyedWorkItem> while our input stream
      // is WindowedValue<SingletonKeyedWorkItem>, which is fine but Java doesn't like it ...
      @SuppressWarnings("unchecked")
      SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<InputT>>>> outDataStream =
          keyedWorkItemStream
              .transform(
                  transform.getName(),
                  outputTypeInfo,
                  (OneInputStreamOperator) doFnOperator);

      context.setOutputDataStream(context.getOutput(transform), outDataStream);

    }
  }

  public static class CombinePerKeyTranslator<K, InputT, OutputT>
      implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<
        Combine.PerKey<K, InputT, OutputT>> {

    @Override
    public void translateNode(
        Combine.PerKey<K, InputT, OutputT> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder = SingletonKeyedWorkItemCoder.of(
          inputKvCoder.getKeyCoder(),
          inputKvCoder.getValueCoder(),
          input.getWindowingStrategy().getWindowFn().windowCoder());

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);


      WindowedValue.ValueOnlyWindowedValueCoder<
            SingletonKeyedWorkItem<K, InputT>> windowedWorkItemCoder =
          WindowedValue.getValueOnlyCoder(workItemCoder);

      CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder);

      DataStream<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new ToKeyedWorkItem<K, InputT>())
              .returns(workItemTypeInfo).name("ToKeyedWorkItem");

      KeyedStream<
            WindowedValue<
                SingletonKeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream = workItemStream
          .keyBy(new WorkItemKeySelector<K, InputT>(inputKvCoder.getKeyCoder()));

      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn = SystemReduceFn.combining(
          inputKvCoder.getKeyCoder(),
          AppliedCombineFn.withInputCoder(
              transform.getFn(), input.getPipeline().getCoderRegistry(), inputKvCoder));


      OldDoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> windowDoFn =
          GroupAlsoByWindowViaWindowSetDoFn.create(windowingStrategy, reduceFn);


      TypeInformation<WindowedValue<KV<K, OutputT>>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      WindowDoFnOperator<K, InputT, KV<K, OutputT>, WindowedValue<KV<K, OutputT>>> doFnOperator =
          new WindowDoFnOperator<>(
              windowDoFn,
              new TupleTag<KV<K, OutputT>>("main output"),
              Collections.<TupleTag<?>>emptyList(),
              new DoFnOperator.DefaultOutputManagerFactory<WindowedValue<KV<K, OutputT>>>(),
              windowingStrategy,
              new HashMap<PCollectionView<?>, WindowingStrategy<?, ?>>(),
              context.getPipelineOptions(),
              inputKvCoder.getKeyCoder());

      // our operator excepts WindowedValue<KeyedWorkItem> while our input stream
      // is WindowedValue<SingletonKeyedWorkItem>, which is fine but Java doesn't like it ...
      @SuppressWarnings("unchecked")
      SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream = keyedWorkItemStream
          .transform(transform.getName(), outputTypeInfo, (OneInputStreamOperator) doFnOperator);

      context.setOutputDataStream(context.getOutput(transform), outDataStream);
    }

    private static class ToKeyedWorkItem<K, InputT>
        extends RichFlatMapFunction<
          WindowedValue<KV<K, InputT>>,
          WindowedValue<SingletonKeyedWorkItem<K, InputT>>> {

      @Override
      public void flatMap(
          WindowedValue<KV<K, InputT>> in,
          Collector<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> out) throws Exception {

        SingletonKeyedWorkItem<K, InputT> workItem =
            new SingletonKeyedWorkItem<>(
                in.getValue().getKey(),
                in.withValue(in.getValue().getValue()));

        out.collect(WindowedValue.valueInEmptyWindows(workItem));
      }
    }
  }

  public static class FlattenPCollectionTranslator<T> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<Flatten.FlattenPCollectionList<T>> {

    @Override
    public void translateNode(Flatten.FlattenPCollectionList<T> transform, FlinkStreamingTranslationContext context) {
      List<PCollection<T>> allInputs = context.getInput(transform).getAll();
      DataStream<T> result = null;
      for (PCollection<T> collection : allInputs) {
        DataStream<T> current = context.getInputDataStream(collection);
        result = (result == null) ? current : result.union(current);
      }
      context.setOutputDataStream(context.getOutput(transform), result);
    }
  }

  public static class ParDoBoundMultiStreamingTranslator<InputT, OutputT>
      implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<
        ParDo.BoundMulti<InputT, OutputT>> {

    @Override
    public void translateNode(
        ParDo.BoundMulti<InputT, OutputT> transform,
        FlinkStreamingTranslationContext context) {

      // we assume that the transformation does not change the windowing strategy.
      WindowingStrategy<?, ?> windowingStrategy =
          context.getInput(transform).getWindowingStrategy();

      Map<TupleTag<?>, PCollection<?>> outputs = context.getOutput(transform).getAll();

      Map<TupleTag<?>, Integer> tagsToLabels =
          transformTupleTagsToLabels(transform.getMainOutputTag(), outputs.keySet());

      DoFnOperator<InputT, OutputT, RawUnionValue> doFnOperator = new DoFnOperator<>(
          transform.getFn(),
          transform.getMainOutputTag(),
          transform.getSideOutputTags().getAll(),
          new DoFnOperator.MultiOutputOutputManagerFactory(tagsToLabels),
          windowingStrategy,
          new HashMap<PCollectionView<?>, WindowingStrategy<?, ?>>(),
          context.getPipelineOptions());

      UnionCoder unionCoder = createUnionCoder(outputs.values());

      CoderTypeInformation<RawUnionValue> unionTypeInformation =
          new CoderTypeInformation<>(unionCoder);

      DataStream<WindowedValue<InputT>> inputDataStream =
          context.getInputDataStream(context.getInput(transform));

      SingleOutputStreamOperator<RawUnionValue> unionStream = inputDataStream
          .transform(transform.getName(), unionTypeInformation, doFnOperator);

      for (Map.Entry<TupleTag<?>, PCollection<?>> output : outputs.entrySet()) {
        final int outputTag = tagsToLabels.get(output.getKey());

        TypeInformation outputTypeInfo =
            context.getTypeInfo(output.getValue());

        unionStream.flatMap(new FlatMapFunction<RawUnionValue, Object>() {
          @Override
          public void flatMap(RawUnionValue value, Collector<Object> out) throws Exception {
            if (value.getUnionTag() == outputTag) {
              out.collect(value.getValue());
            }
          }
        }).returns(outputTypeInfo);
      }
    }

    private Map<TupleTag<?>, Integer> transformTupleTagsToLabels(TupleTag<?> mainTag, Set<TupleTag<?>> secondaryTags) {
      Map<TupleTag<?>, Integer> tagToLabelMap = Maps.newHashMap();
      int count = 0;
      tagToLabelMap.put(mainTag, count++);
      for (TupleTag<?> tag : secondaryTags) {
        if (!tagToLabelMap.containsKey(tag)) {
          tagToLabelMap.put(tag, count++);
        }
      }
      return tagToLabelMap;
    }

    private UnionCoder createUnionCoder(Collection<PCollection<?>> taggedCollections) {
      List<Coder<?>> outputCoders = Lists.newArrayList();
      for (PCollection<?> coll : taggedCollections) {
        outputCoders.add(coll.getCoder());
      }
      return UnionCoder.of(outputCoders);
    }
  }
}
