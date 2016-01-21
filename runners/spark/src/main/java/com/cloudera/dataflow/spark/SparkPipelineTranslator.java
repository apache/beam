/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.transforms.PTransform;

/**
 * Translator to support translation between Dataflow transformations and Spark transformations.
 */
public interface SparkPipelineTranslator {

  boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz);

  <PT extends PTransform<?, ?>> TransformEvaluator<PT> translate(Class<PT> clazz);
}
