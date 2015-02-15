package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.TextIO.Read.Bound;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;

import java.util.HashMap;
import java.util.Map;


public class FlinkTransformTranslators {

	// --------------------------------------------------------------------------------------------
	//  Transform Translator Registry
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("rawtypes")
	private static final Map<Class<? extends PTransform>, TransformToFlinkOpTranslator> TRANSLATORS =
			new HashMap<Class<? extends PTransform>, TransformToFlinkOpTranslator>();
	
	// register the known translators
	static {
		TRANSLATORS.put(TextIO.Read.Bound.class, new ReadUTFTextTranslator());
		TRANSLATORS.put(ParDo.Bound.class, new ParallelDoTranslator());
		TRANSLATORS.put(GroupByKey.GroupByKeyOnly.class, new GroupByKeyOnlyTranslator());
	}
	
	
	public static TransformToFlinkOpTranslator<?> getTranslator(PTransform<?, ?> transform) {
		return TRANSLATORS.get(transform.getClass());
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Individual Transform Translators
	// --------------------------------------------------------------------------------------------
	
	private static class ReadUTFTextTranslator implements TransformToFlinkOpTranslator<TextIO.Read.Bound<String>> {
		
		@Override
		public void translateNode(TransformTreeNode node, Bound<String> transform, TranslationContext context) {
			System.out.println("Translating " + node.getFullName());
			
			String path = transform.getFilepattern();
			String name = transform.getName(); 
			Coder<?> coder = transform.getDefaultOutputCoder(transform.getOutput());
			
			if (coder != null && coder != TextIO.DEFAULT_TEXT_CODER) {
				throw new UnsupportedOperationException("Currently only supports UTF-8 inputs.");
			}
			
			DataSource<String> source = context.getExecutionEnvironment().readTextFile(path);
			if (name != null) {
				source = source.name(name);
			}
			context.setOutputDataSet(node, source);
		}
	}
	
	private static class GroupByKeyOnlyTranslator <K,V> implements TransformToFlinkOpTranslator<GroupByKey.GroupByKeyOnly<K,V>> {

		@Override
		public void translateNode(TransformTreeNode node, GroupByKey.GroupByKeyOnly transform, TranslationContext context) {
			DataSet<KV<K,V>> dataSet = (DataSet<KV<K, V>>) context.getInputDataSet(node);
			dataSet.groupBy(new KeySelector<KV<K,V>, K>() {
				@Override
				public K getKey(KV<K, V> kv) throws Exception {
					return kv.getKey();
				}
			});
			context.setOutputDataSet(node, dataSet);
		}
	}
	
	private static class ParallelDoTranslator<IN, OUT> implements TransformToFlinkOpTranslator<ParDo.Bound<IN, OUT>> {
		
		@Override
		public void translateNode(TransformTreeNode node, ParDo.Bound<IN, OUT> transform, TranslationContext context) {
			System.out.println("Translating " + node.getFullName());

			ExecutionEnvironment env = context.getExecutionEnvironment();
			DataSet<IN> dataSet = (DataSet<IN>) context.getInputDataSet(node);
			
			final DoFn<IN, OUT> doFn = transform.getFn();
			dataSet.mapPartition(new FlinkDoFnFunction<>(doFn));
			
			context.setOutputDataSet(node, dataSet);
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private FlinkTransformTranslators() {}
}
