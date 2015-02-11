package com.dataartisans.flink.dataflow.translation;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.operators.DataSource;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.TextIO.Read.Bound;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;


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
		TRANSLATORS.put(ParDo.Bound.class, null);
	}
	
	
	public static TransformToFlinkOpTranslator<?> getTranslator(PTransform<?, ?> transform) {
		return TRANSLATORS.get(transform.getClass());
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Individual Transform Translators
	// --------------------------------------------------------------------------------------------
	
	private static class ReadUTFTextTranslator implements TransformToFlinkOpTranslator<TextIO.Read.Bound<String>> {
		
		@Override
		public void translateNode(TransformTreeNode node, Bound<String> transform, TranslationContext translation) {
			String path = transform.getFilepattern();
			String name = transform.getName(); 
			Coder<?> coder = transform.getDefaultOutputCoder(transform.getOutput());
			
			if (coder != null && coder != TextIO.DEFAULT_TEXT_CODER) {
				throw new UnsupportedOperationException("Currently only supports UTF-8 inputs.");
			}
			
			DataSource<String> source = translation.getExecutionEnvironment().readTextFile(path);
			if (name != null) {
				source = source.name(name);
			}
			
			translation.registerDataSet(source, node);
		}
	}
	
	private static class ParallelDoTranslator<IN, OUT> implements TransformToFlinkOpTranslator<ParDo.Bound<IN, OUT>> {
		
		@Override
		public void translateNode(TransformTreeNode node, ParDo.Bound<IN, OUT> transform, TranslationContext context) {
			
			
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private FlinkTransformTranslators() {}
}
