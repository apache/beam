package com.dataartisans.flink.dataflow.translation;

import com.dataartisans.flink.dataflow.translation.functions.FlinkCreateFunction;
import com.dataartisans.flink.dataflow.translation.functions.FlinkDoFnFunction;
import com.dataartisans.flink.dataflow.translation.functions.KeyedListAggregator;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.core.fs.Path;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Translators for transforming
 * Dataflow {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s to
 * Flink {@link org.apache.flink.api.java.DataSet}s
 */
public class FlinkTransformTranslators {

	// --------------------------------------------------------------------------------------------
	//  Transform Translator Registry
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("rawtypes")
	private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS = new HashMap<>();
	
	// register the known translators
	static {
		TRANSLATORS.put(View.CreatePCollectionView.class, new CreatePCollectionViewTranslator());
		//TRANSLATORS.put(Combine.GroupedValues.class, null);
		TRANSLATORS.put(Create.class, new CreateTranslator());
		TRANSLATORS.put(Flatten.FlattenPCollectionList.class, new FlattenPCollectionTranslator());
		TRANSLATORS.put(GroupByKey.GroupByKeyOnly.class, new GroupByKeyOnlyTranslator());
		//TRANSLATORS.put(ParDo.BoundMulti.class, null);
		TRANSLATORS.put(ParDo.Bound.class, new ParallelDoTranslator());

		//TRANSLATORS.put(AvroIO.Read.Bound.class, null);
		//TRANSLATORS.put(AvroIO.Write.Bound.class, null);

		//TRANSLATORS.put(BigQueryIO.Read.Bound.class, null);
		//TRANSLATORS.put(BigQueryIO.Write.Bound.class, null);

		//TRANSLATORS.put(DatastoreIO.Sink.class, null);

		//TRANSLATORS.put(PubsubIO.Read.Bound.class, null);
		//TRANSLATORS.put(PubsubIO.Write.Bound.class, null);

		//TRANSLATORS.put(ReadSource.Bound.class, null);

		TRANSLATORS.put(TextIO.Read.Bound.class, new TextIOReadTranslator());
		TRANSLATORS.put(TextIO.Write.Bound.class, new TextIOWriteTranslator());
	}
	
	
	public static TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
		return TRANSLATORS.get(transform.getClass());
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Individual Transform Translators
	// --------------------------------------------------------------------------------------------
	
	private static class TextIOReadTranslator implements TransformTranslator<TextIO.Read.Bound<String>> {
		
		@Override
		public void translateNode(TransformTreeNode node, TextIO.Read.Bound<String> transform, TranslationContext context) {
			String path = transform.getFilepattern();
			String name = transform.getName(); 

			TypeInformation<String> typeInformation = context.getTypeInfo(transform.getOutput());

			DataSource<String> source = new DataSource<String>(context.getExecutionEnvironment(), new TextInputFormat(new Path(path)), typeInformation, name);

			context.setOutputDataSet(transform.getOutput(), source);
		}
	}

	private static class TextIOWriteTranslator<T> implements TransformTranslator<TextIO.Write.Bound<T>> {
		
		@Override
		public void translateNode(TransformTreeNode node, TextIO.Write.Bound<T> transform, TranslationContext context) {
			DataSet<T> dataSet = context.getInputDataSet(transform.getInput());
			String path = transform.getFilenamePrefix();

			dataSet.writeAsText(path);
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

	private static class CreatePCollectionViewTranslator<R, T, WT> implements TransformTranslator<View.CreatePCollectionView<R,T,WT>> {
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
			DataSet<Integer> initDataSet = context.getExecutionEnvironment().fromElements(1);
			FlinkCreateFunction<Integer, OUT> flatMapFunction = new FlinkCreateFunction<>(Lists.newArrayList(elements));
			FlatMapOperator<Integer, OUT> dataSetNew = new FlatMapOperator<>(initDataSet, typeInformation, flatMapFunction, transform.getName());
			
			context.setOutputDataSet(transform.getOutput(), dataSetNew);
		}
	}

		// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private FlinkTransformTranslators() {}
}
