package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.TextIO.Read.Bound;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.operators.Keys;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FlinkTransformTranslators {

	// --------------------------------------------------------------------------------------------
	//  Transform Translator Registry
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("rawtypes")
	private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS = new HashMap<>();
	
	// register the known translators
	static {
		TRANSLATORS.put(TextIO.Read.Bound.class, new ReadUTFTextTranslator());
		TRANSLATORS.put(TextIO.Write.Bound.class, new WriteUTFTextTranslator());
		TRANSLATORS.put(ParDo.Bound.class, new ParallelDoTranslator());
		TRANSLATORS.put(GroupByKey.GroupByKeyOnly.class, new GroupByKeyOnlyTranslator());
		TRANSLATORS.put(Flatten.FlattenPCollectionList.class, new FlattenPCollectionTranslator());
		TRANSLATORS.put(View.CreatePCollectionView.class, new PCollectionViewTranslator());
		TRANSLATORS.put(Create.class, new CreateTranslator());
	}
	
	
	public static TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
		return TRANSLATORS.get(transform.getClass());
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Individual Transform Translators
	// --------------------------------------------------------------------------------------------
	
	private static class ReadUTFTextTranslator implements TransformTranslator<Bound<String>> {
		
		@Override
		public void translateNode(TransformTreeNode node, Bound<String> transform, TranslationContext context) {
			System.out.println("Translating " + node.getFullName());
			
			String path = transform.getFilepattern();
			String name = transform.getName(); 
			Coder<?> coder = transform.getOutput().getCoder();

			if (coder != null && coder != TextIO.DEFAULT_TEXT_CODER) {
				throw new UnsupportedOperationException("Currently only supports UTF-8 inputs.");
			}
			
			DataSource<String> source = context.getExecutionEnvironment().readTextFile(path);
			if (name != null) {
				source.name(name);
			}
			context.setOutputDataSet(transform.getOutput(), source);
		}
	}

	private static class WriteUTFTextTranslator <T> implements TransformTranslator<TextIO.Write.Bound<T>> {
		
		@Override
		public void translateNode(TransformTreeNode node, TextIO.Write.Bound<T> transform, TranslationContext context) {
			DataSet<T> dataSet = context.getInputDataSet(transform.getInput());
			dataSet.print();
		}
	}
	
	private static class GroupByKeyOnlyTranslator <K,V> implements TransformTranslator<GroupByKey.GroupByKeyOnly<K,V>> {

		@Override
		public void translateNode(TransformTreeNode node, GroupByKey.GroupByKeyOnly<K,V> transform, TranslationContext context) {
			DataSet<KV<K,V>> dataSet = context.getInputDataSet(transform.getInput());
			GroupReduceFunction<KV<K, V>, KV<K, Iterable<V>>> groupReduceFunction = new KeyedListAggregator<>();
			
			TypeInformation<KV<K, Iterable<V>>> typeInformation = context.getTypeInfo(transform.getOutput());

			Grouping<KV<K, V>> grouping = new UnsortedGrouping<>(dataSet, new Keys.ExpressionKeys<>(new String[]{""}, dataSet.getType()));

			GroupReduceOperator<KV<K, V>, KV<K, Iterable<V>>> dataSetNew = 
					new GroupReduceOperator<>(grouping, typeInformation, groupReduceFunction, transform.getName());
			context.setOutputDataSet(transform.getOutput(), dataSetNew);
		}
	}
	
	private static class ParallelDoTranslator<IN, OUT, T, WT> implements TransformTranslator<ParDo.Bound<IN, OUT>> {
		
		@Override
		public void translateNode(TransformTreeNode node, ParDo.Bound<IN, OUT> transform, TranslationContext context) {
			DataSet<IN> dataSet = context.getInputDataSet(transform.getInput());

			final DoFn<IN, OUT> doFn = transform.getFn();
			
			TypeInformation<OUT> typeInformation = context.getTypeInfo(transform.getOutput());

			FlinkDoFnFunction<IN, OUT> mapPartitionFunction = new FlinkDoFnFunction<>(doFn);
			MapPartitionOperator<IN, OUT> dataSetNew = new MapPartitionOperator<>(dataSet, typeInformation, mapPartitionFunction, transform.getName());

			List<PCollectionView<?, ?>> sideInputs = transform.getSideInputs();
			// get corresponding Flink broadcast data sets
			for(PCollectionView<?, ?> input : sideInputs) {
				DataSet<T> broadcastSet = context.getSideInputDataSet(input);
				dataSetNew.withBroadcastSet(broadcastSet, input.getTagInternal().getId());
			}

			context.setOutputDataSet(transform.getOutput(), dataSetNew);
		}
	}

	private static class FlattenPCollectionTranslator<T> implements TransformTranslator<Flatten.FlattenPCollectionList<T>> {

		@Override
		public void translateNode(TransformTreeNode node, Flatten.FlattenPCollectionList<T> transform, TranslationContext context) {
			List<PCollection<T>> allInputs = transform.getInput().getAll();
			DataSet<T> result = null;
			for(PCollection<T> collection : allInputs) {
				DataSet<T> current = context.getInputDataSet(collection);
				if (result == null) {
					result = current;
				} else {
					result = result.union(current);
				}
			}
			context.setOutputDataSet(transform.getOutput(), result);
		}
	}

	private static class PCollectionViewTranslator<R, T, WT> implements TransformTranslator<View.CreatePCollectionView<R,T,WT>> {
		@Override
		public void translateNode(TransformTreeNode node, View.CreatePCollectionView<R,T,WT> transform, TranslationContext context) {
			DataSet<T> dataSet = context.getInputDataSet(transform.getInput());
			PCollectionView<T, WT> input = transform.apply(null);
			context.setSideInputDataSet(input, dataSet);
		}
	}

	private static class CreateTranslator<OUT> implements TransformTranslator<Create<OUT>> {

		@Override
		public void translateNode(TransformTreeNode node, Create<OUT> transform, TranslationContext context) {
			TypeInformation<OUT> typeInformation = context.getTypeInfo(transform.getOutput());
			Iterable<OUT> elements = transform.getElements();
			for(OUT elem : elements) {
				System.out.println("element:"+elem);
				System.out.println("type: " + typeInformation);
			}
			DataSet<Integer> initDataSet = context.getExecutionEnvironment().fromElements(1);
			FlinkCreateFlatMap<Integer, OUT> flatMapFunction = new FlinkCreateFlatMap<>(Lists.newArrayList(elements));
			FlatMapOperator<Integer, OUT> dataSetNew = new FlatMapOperator<>(initDataSet, typeInformation, flatMapFunction, transform.getName());
			
			context.setOutputDataSet(transform.getOutput(), dataSetNew);
		}
	}

		// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private FlinkTransformTranslators() {}
}
