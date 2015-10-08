/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineVisitor;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * FlinkPipelineTranslator knows how to translate Pipeline objects into Flink Jobs.
 *
 * This is based on {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator}
 */
public class FlinkPipelineTranslator implements PipelineVisitor {

	private final TranslationContext context;

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
		PTransform<?, ?> transform = node.getTransform();

		if (transform != null) {
			TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(transform);

			if (translator != null) {
				inComposite = true;

				if (transform instanceof CoGroupByKey && node.getInput().expand().size() != 2) {
					// we can only optimize CoGroupByKey for input size 2
					inComposite = false;
				}
			}
		}

		this.depth++;
	}

	@Override
	public void leaveCompositeTransform(TransformTreeNode node) {
		PTransform<?, ?> transform = node.getTransform();

		if (transform != null) {
			TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(transform);

			if (translator != null) {
				System.out.println(genSpaces(this.depth) + "doingCompositeTransform- " + formatNodeName(node));
				applyTransform(transform, node, translator);
				inComposite = false;
			}
		}

		this.depth--;
		System.out.println(genSpaces(this.depth) + "leaveCompositeTransform- " + formatNodeName(node));
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

		// create the applied PTransform on the context
		context.setCurrentTransform(AppliedPTransform.of(
				node.getFullName(), node.getInput(), node.getOutput(), (PTransform) transform));

		typedTranslator.translateNode(typedTransform, context);
	}

	/**
	 * A translator of a {@link PTransform}.
	 */
	public interface TransformTranslator<Type extends PTransform> {

		void translateNode(Type transform, TranslationContext context);
	}
}
