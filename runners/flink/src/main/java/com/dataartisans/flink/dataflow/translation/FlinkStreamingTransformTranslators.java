/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.dataflow.translation;

import com.dataartisans.flink.dataflow.translation.functions.UnionCoder;
import com.dataartisans.flink.dataflow.translation.types.CoderTypeInformation;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.*;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.io.FlinkStreamingCreateFunction;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.io.UnboundedFlinkSource;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.join.RawUnionValue;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

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
		TRANSLATORS.put(Read.Unbounded.class, new UnboundedReadSourceTranslator());
		TRANSLATORS.put(ParDo.Bound.class, new ParDoBoundStreamingTranslator());
		TRANSLATORS.put(TextIO.Write.Bound.class, new TextIOWriteBoundStreamingTranslator());
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
			Coder<OUT> elementCoder = context.getOutput(transform).getCoder();
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

			context.setOutputDataStream(context.getOutput(transform), outputDataStream);
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

	private static class UnboundedReadSourceTranslator<T> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<Read.Unbounded<T>> {

		@Override
		public void translateNode(Read.Unbounded<T> transform, FlinkStreamingTranslationContext context) {
			PCollection<T> output = context.getOutput(transform);

			DataStream<WindowedValue<T>> source;
			if (transform.getSource().getClass().equals(UnboundedFlinkSource.class)) {
				UnboundedFlinkSource flinkSource = (UnboundedFlinkSource) transform.getSource();
				source = context.getExecutionEnvironment()
						.addSource(flinkSource.getFlinkSource())
						.flatMap(new FlatMapFunction<String, WindowedValue<String>>() {
							@Override
							public void flatMap(String s, Collector<WindowedValue<String>> collector) throws Exception {
								collector.collect(WindowedValue.<String>of(s, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
							}
						});
			} else {
				source = context.getExecutionEnvironment()
						.addSource(new UnboundedSourceWrapper<>(context.getPipelineOptions(), transform));
			}
			context.setOutputDataStream(output, source);
		}
	}

	private static class ParDoBoundStreamingTranslator<IN, OUT> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<ParDo.Bound<IN, OUT>> {

		@Override
		public void translateNode(ParDo.Bound<IN, OUT> transform, FlinkStreamingTranslationContext context) {
			PCollection<OUT> output = context.getOutput(transform);

			final WindowingStrategy<OUT, ? extends BoundedWindow> windowingStrategy =
					(WindowingStrategy<OUT, ? extends BoundedWindow>)
							context.getOutput(transform).getWindowingStrategy();

			WindowedValue.WindowedValueCoder<OUT> outputStreamCoder = WindowedValue.getFullCoder(output.getCoder(),
					windowingStrategy.getWindowFn().windowCoder());
			CoderTypeInformation<WindowedValue<OUT>> outputWindowedValueCoder =
					new CoderTypeInformation<>(outputStreamCoder);

			FlinkParDoBoundWrapper<IN, OUT> doFnWrapper = new FlinkParDoBoundWrapper<>(
					context.getPipelineOptions(), windowingStrategy, transform.getFn());
			DataStream<WindowedValue<IN>> inputDataStream = context.getInputDataStream(context.getInput(transform));
			SingleOutputStreamOperator<WindowedValue<OUT>> outDataStream = inputDataStream.flatMap(doFnWrapper)
					.returns(outputWindowedValueCoder);

			context.setOutputDataStream(context.getOutput(transform), outDataStream);
		}
	}

	public static class WindowBoundTranslator<T> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<Window.Bound<T>> {

		@Override
		public void translateNode(Window.Bound<T> transform, FlinkStreamingTranslationContext context) {
			PValue input = context.getInput(transform);
			DataStream<WindowedValue<T>> inputDataStream = context.getInputDataStream(input);

			final WindowingStrategy<T, ? extends BoundedWindow> windowingStrategy =
					(WindowingStrategy<T, ? extends BoundedWindow>)
							context.getOutput(transform).getWindowingStrategy();

			final WindowFn<T, ? extends BoundedWindow> windowFn = windowingStrategy.getWindowFn();

			WindowedValue.WindowedValueCoder<T> outputStreamCoder = WindowedValue.getFullCoder(
					context.getInput(transform).getCoder(), windowingStrategy.getWindowFn().windowCoder());
			CoderTypeInformation<WindowedValue<T>> outputWindowedValueCoder =
					new CoderTypeInformation<>(outputStreamCoder);

			final FlinkParDoBoundWrapper<T, T> windowDoFnAssigner = new FlinkParDoBoundWrapper<>(
					context.getPipelineOptions(), windowingStrategy, createWindowAssigner(windowFn));

			SingleOutputStreamOperator<WindowedValue<T>> windowedStream =
					inputDataStream.flatMap(windowDoFnAssigner).returns(outputWindowedValueCoder);
			context.setOutputDataStream(context.getOutput(transform), windowedStream);
		}

		private static <T, W extends BoundedWindow> DoFn<T, T> createWindowAssigner(final WindowFn<T, W> windowFn) {
			return new DoFn<T, T>() {

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
								public Collection<? extends BoundedWindow> windows() {
									return c.windowingInternals().windows();
								}
							});

					c.windowingInternals().outputWindowedValue(
							c.element(), c.timestamp(), windows, c.pane());
				}
			};
		}
	}

	public static class GroupByKeyTranslator<K, V> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<GroupByKey<K, V>> {

		@Override
		public void translateNode(GroupByKey<K, V> transform, FlinkStreamingTranslationContext context) {
			PValue input = context.getInput(transform);

			DataStream<WindowedValue<KV<K, V>>> inputDataStream = context.getInputDataStream(input);
			KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) context.getInput(transform).getCoder();

			KeyedStream<WindowedValue<KV<K, V>>, K> groupByKStream = FlinkGroupByKeyWrapper
					.groupStreamByKey(inputDataStream, inputKvCoder);

			DataStream<WindowedValue<KV<K, Iterable<V>>>> groupedByKNWstream =
					FlinkGroupAlsoByWindowWrapper.createForIterable(context.getPipelineOptions(),
							context.getInput(transform), groupByKStream);

			context.setOutputDataStream(context.getOutput(transform), groupedByKNWstream);
		}
	}

	public static class CombinePerKeyTranslator<K, VIN, VACC, VOUT> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<Combine.PerKey<K, VIN, VOUT>> {

		@Override
		public void translateNode(Combine.PerKey<K, VIN, VOUT> transform, FlinkStreamingTranslationContext context) {
			PValue input = context.getInput(transform);

			DataStream<WindowedValue<KV<K, VIN>>> inputDataStream = context.getInputDataStream(input);
			KvCoder<K, VIN> inputKvCoder = (KvCoder<K, VIN>) context.getInput(transform).getCoder();
			KvCoder<K, VOUT> outputKvCoder = (KvCoder<K, VOUT>) context.getOutput(transform).getCoder();

			KeyedStream<WindowedValue<KV<K, VIN>>, K> groupByKStream = FlinkGroupByKeyWrapper
					.groupStreamByKey(inputDataStream, inputKvCoder);

			Combine.KeyedCombineFn<K, VIN, VACC, VOUT> combineFn = (Combine.KeyedCombineFn<K, VIN, VACC, VOUT>) transform.getFn();
			DataStream<WindowedValue<KV<K, VOUT>>> groupedByKNWstream =
					FlinkGroupAlsoByWindowWrapper.create(context.getPipelineOptions(),
							context.getInput(transform), groupByKStream, combineFn, outputKvCoder);

			context.setOutputDataStream(context.getOutput(transform), groupedByKNWstream);
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

	public static class ParDoBoundMultiStreamingTranslator<IN, OUT> implements FlinkStreamingPipelineTranslator.StreamTransformTranslator<ParDo.BoundMulti<IN, OUT>> {

		private final int MAIN_TAG_INDEX = 0;

		@Override
		public void translateNode(ParDo.BoundMulti<IN, OUT> transform, FlinkStreamingTranslationContext context) {

			// we assume that the transformation does not change the windowing strategy.
			WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy = context.getInput(transform).getWindowingStrategy();

			Map<TupleTag<?>, PCollection<?>> outputs = context.getOutput(transform).getAll();
			Map<TupleTag<?>, Integer> tagsToLabels = transformTupleTagsToLabels(
					transform.getMainOutputTag(), outputs.keySet());

			UnionCoder intermUnionCoder = getIntermUnionCoder(outputs.values());
			WindowedValue.WindowedValueCoder<RawUnionValue> outputStreamCoder = WindowedValue.getFullCoder(
					intermUnionCoder, windowingStrategy.getWindowFn().windowCoder());

			CoderTypeInformation<WindowedValue<RawUnionValue>> intermWindowedValueCoder =
					new CoderTypeInformation<>(outputStreamCoder);

			FlinkParDoBoundMultiWrapper<IN, OUT> doFnWrapper = new FlinkParDoBoundMultiWrapper<>(
					context.getPipelineOptions(), windowingStrategy, transform.getFn(),
					transform.getMainOutputTag(), tagsToLabels);

			DataStream<WindowedValue<IN>> inputDataStream = context.getInputDataStream(context.getInput(transform));
			SingleOutputStreamOperator<WindowedValue<RawUnionValue>> intermDataStream =
					inputDataStream.flatMap(doFnWrapper).returns(intermWindowedValueCoder);

			for (Map.Entry<TupleTag<?>, PCollection<?>> output : outputs.entrySet()) {
				final int outputTag = tagsToLabels.get(output.getKey());

				WindowedValue.WindowedValueCoder<?> coderForTag = WindowedValue.getFullCoder(
						output.getValue().getCoder(),
						windowingStrategy.getWindowFn().windowCoder());

				CoderTypeInformation<WindowedValue<?>> windowedValueCoder =
						new CoderTypeInformation(coderForTag);

				context.setOutputDataStream(output.getValue(),
						intermDataStream.filter(new FilterFunction<WindowedValue<RawUnionValue>>() {
							@Override
							public boolean filter(WindowedValue<RawUnionValue> value) throws Exception {
								return value.getValue().getUnionTag() == outputTag;
							}
						}).flatMap(new FlatMapFunction<WindowedValue<RawUnionValue>, WindowedValue<?>>() {
							@Override
							public void flatMap(WindowedValue<RawUnionValue> value, Collector<WindowedValue<?>> collector) throws Exception {
								collector.collect(WindowedValue.of(
										value.getValue().getValue(),
										value.getTimestamp(),
										value.getWindows(),
										value.getPane()));
							}
						}).returns(windowedValueCoder));
			}
		}

		private Map<TupleTag<?>, Integer> transformTupleTagsToLabels(TupleTag<?> mainTag, Set<TupleTag<?>> secondaryTags) {
			Map<TupleTag<?>, Integer> tagToLabelMap = Maps.newHashMap();
			tagToLabelMap.put(mainTag, MAIN_TAG_INDEX);
			int count = MAIN_TAG_INDEX + 1;
			for (TupleTag<?> tag : secondaryTags) {
				if (!tagToLabelMap.containsKey(tag)) {
					tagToLabelMap.put(tag, count++);
				}
			}
			return tagToLabelMap;
		}

		private UnionCoder getIntermUnionCoder(Collection<PCollection<?>> taggedCollections) {
			List<Coder<?>> outputCoders = Lists.newArrayList();
			for (PCollection<?> coll : taggedCollections) {
				outputCoders.add(coll.getCoder());
			}
			return UnionCoder.of(outputCoders);
		}
	}
}
