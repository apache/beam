package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineVisitor;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PValue;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * FlinkPipelineTranslator knows how to translate Pipeline objects into Flink Jobs.
 *
 * This is based on {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator}
 */
public class FlinkPipelineTranslator implements PipelineVisitor {
	
	private final TranslationContext context;
	
	private boolean inOptimizableCoGroupByKey = false;
	
	private int depth = 0;

	private boolean inComposite = false;

	public FlinkPipelineTranslator(ExecutionEnvironment env, PipelineOptions options) {
		this.context = new TranslationContext(env, options);
	}

	
	public void translate(Pipeline pipeline) {
		pipeline.traverseTopologically(this);
	}
	

	// --------------------------------------------------------------------------------------------
	//  Pipeline Visitor Methods
	// --------------------------------------------------------------------------------------------
	
	private static String genSpaces(int n) {
		String s = "";
		for(int i = 0; i < n; i++) {
			s += "|   ";
		}
		return s;
	}
	
	private static String formatNodeName(TransformTreeNode node) {
		return node.toString().split("@")[1] + node.getTransform();
	}
	
	@Override
	public void enterCompositeTransform(TransformTreeNode node) {
		System.out.println(genSpaces(this.depth) + "enterCompositeTransform- " + formatNodeName(node));

		if (node.getTransform() != null) {
			TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(node.getTransform());

			if (translator != null) {
				inComposite = true;
			}
		}

		this.depth++;
		
		PTransform<?, ?> transform = node.getTransform();
		if (transform instanceof CoGroupByKey) {
			// Check input size
			KeyedPCollectionTuple<?> input = ((CoGroupByKey<?>) transform).getInput();
			if(input.getKeyedCollections().size() == 2) {

				inOptimizableCoGroupByKey = true;
				// optimize
				TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(transform);
				applyTransform(transform, node, translator);
			}
		}
	}

	@Override
	public void leaveCompositeTransform(TransformTreeNode node) {

		if (node.getTransform() != null) {
			TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(node.getTransform());

			if (translator != null) {
				System.out.println(genSpaces(this.depth) + "doingCompositeTransform- " + formatNodeName(node));
				applyTransform(node.getTransform(), node, translator);
				inComposite = false;
			}
		}

		this.depth--;
		System.out.println(genSpaces(this.depth) + "leaveCompositeTransform- " + formatNodeName(node));

		PTransform<?, ?> transform = node.getTransform();
		if (transform instanceof CoGroupByKey) {
			inOptimizableCoGroupByKey = false;
		}
	}

	@Override
	public void visitTransform(TransformTreeNode node) {
		System.out.println(genSpaces(this.depth) + "visitTransform- " + formatNodeName(node));
		if (inComposite) {
			// ignore it
			return;
		}

		// the transformation applied in this node
		PTransform<?, ?> transform = node.getTransform();
		
		// the translator to the Flink operation(s)
		TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(transform);
		
		if (translator == null) {
			System.out.println(node.getTransform().getClass());
			throw new UnsupportedOperationException("The transform " + transform + " is currently not supported.");
		}

		if (!inOptimizableCoGroupByKey) {
			applyTransform(transform, node, translator);
		}

		applyTransform(transform, node, translator);
	}

	@Override
	public void visitValue(PValue value, TransformTreeNode producer) {
		// do nothing here
	}
	
	/**
	 * Utility method to define a generic variable to cast the translator and the transform to.
	 */
	private <T extends PTransform<?, ?>> void applyTransform(PTransform<?, ?> transform, TransformTreeNode node, TransformTranslator<?> translator) {
		
		@SuppressWarnings("unchecked")
		T typedTransform = (T) transform;
		
		@SuppressWarnings("unchecked")
		TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
		
		typedTranslator.translateNode(typedTransform, context);
	}

	/**
	 * A translator of a {@link PTransform}.
	 */
	public static interface TransformTranslator<Type extends PTransform> {

		void translateNode(Type transform, TranslationContext context);
	}
}
