/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.logging.Logger;

/**
 * The SparkPipelineRunner translate operations defined on a pipeline to a representation
 * executable by Spark, and then submitting the job to Spark to be executed. If we wanted to run
 * a dataflow pipeline with the default options of a single threaded spark instance in local mode,
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * EvaluationResult result = SparkPipelineRunner.create().run(p);
 * }
 *
 * To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * EvaluationResult result = SparkPipelineRunner.create(options).run(p);
 * }
 */
public class SparkPipelineRunner extends PipelineRunner<EvaluationResult> {

  private static final Logger LOG = Logger.getLogger(SparkPipelineRunner.class.getName());
  /**
   * Options used in this pipeline runner.
   */
  private final SparkPipelineOptions mOptions;

  /**
   * Creates and returns a new SparkPipelineRunner with default options. In particular, against a
   * spark instance running in local mode.
   *
   * @return A pipeline runner with default options.
   */
  public static SparkPipelineRunner create() {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    return new SparkPipelineRunner(options);
  }

  /**
   * Creates and returns a new SparkPipelineRunner with specified options.
   *
   * @param options The SparkPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkPipelineRunner create(SparkPipelineOptions options) {
    return new SparkPipelineRunner(options);
  }


  /**
   * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
   * thread.
   */
  private SparkPipelineRunner(SparkPipelineOptions options) {
    mOptions = options;
  }


  @Override
  public EvaluationResult run(Pipeline pipeline) {
    JavaSparkContext jsc = getContext();
    EvaluationContext ctxt = new EvaluationContext(jsc, pipeline);
    pipeline.traverseTopologically(new Evaluator(ctxt));
    return ctxt;
  }

  private JavaSparkContext getContext() {
    SparkConf conf = new SparkConf();
    conf.setMaster(mOptions.getSparkMaster());
    conf.setAppName("spark pipeline job");
    conf.set("sun.io.serialization.extendeddebuginfo", "true");
    return new JavaSparkContext(conf);
  }

  private static class Evaluator implements Pipeline.PipelineVisitor {

    private final EvaluationContext ctxt;

    // Set upon entering a composite node which can be directly mapped to a single
    // TransformEvaluator.
    private TransformTreeNode currentTranslatedCompositeNode = null;

    private Evaluator(EvaluationContext ctxt) {
      this.ctxt = ctxt;
    }

    /**
     * If true, we're currently inside a subtree of a composite node which directly maps to a
     * single TransformEvaluator; children nodes are ignored, and upon post-visiting the translated
     * composite node, the associated TransformEvaluator will be visited.
     */
    private boolean inTranslatedCompositeNode() {
      return currentTranslatedCompositeNode != null;
    }

    @Override
    public void enterCompositeTransform(TransformTreeNode node) {
      if (inTranslatedCompositeNode()) {
        return;
      }

      if (node.getTransform() != null
          && TransformTranslator.hasTransformEvaluator(node.getTransform().getClass())) {
        LOG.info(String.format(
            "Entering directly-translatable composite transform: '%s'", node.getFullName()));
        LOG.fine(String.format(
            "Composite transform class: '%s'", node.getTransform().getClass()));
        currentTranslatedCompositeNode = node;
      }
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {
      // NB: We depend on enterCompositeTransform and leaveCompositeTransform providing 'node'
      // objects for which Object.equals() returns true iff they are the same logical node
      // within the tree.
      if (inTranslatedCompositeNode() && node.equals(currentTranslatedCompositeNode)) {
        LOG.info(String.format(
            "Post-visiting directly-translatable composite transform: '%s'", node.getFullName()));
        doVisitTransform(node.getTransform());
        currentTranslatedCompositeNode = null;
      }
    }

    @Override
    public void visitTransform(TransformTreeNode node) {
      if (inTranslatedCompositeNode()) {
        LOG.info(String.format(
            "Skipping '%s'; already in composite transform.", node.getFullName()));
        return;
      }
      doVisitTransform(node.getTransform());
    }

    private <PT extends PTransform> void doVisitTransform(PT transform) {
      @SuppressWarnings("unchecked")
      TransformEvaluator<PT> evaluator = (TransformEvaluator<PT>)
          TransformTranslator.getTransformEvaluator(transform.getClass());
      LOG.info("Evaluating " + transform);
      evaluator.evaluate(transform, ctxt);
    }

    @Override
    public void visitValue(PValue pvalue, TransformTreeNode node) {
    }
  }
}

