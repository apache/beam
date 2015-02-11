package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.PTransform;


public interface TransformToFlinkOpTranslator<Type extends PTransform<?, ?>> {

	void translateNode(TransformTreeNode node, Type transform, TranslationContext context);
}
