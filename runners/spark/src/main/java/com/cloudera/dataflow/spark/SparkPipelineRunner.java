/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.dataflow.spark;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.logging.Logger;

public class SparkPipelineRunner extends PipelineRunner<SparkPipelineRunner.EvaluationResult> {

  private static final Logger LOG =
      Logger.getLogger(SparkPipelineRunner.class.getName());

  private final String master;

  public SparkPipelineRunner() {
    this("local");
  }

  public SparkPipelineRunner(String master) {
    this.master = Preconditions.checkNotNull(master);
  }

  @Override
  public EvaluationResult run(Pipeline pipeline) {
    EvaluationContext ctxt = new EvaluationContext(this.master);
    pipeline.traverseTopologically(new Evaluator(ctxt));
    return ctxt;
  }

  private class Evaluator implements Pipeline.PipelineVisitor {

    private final EvaluationContext ctxt;

    private Evaluator(EvaluationContext ctxt) {
      this.ctxt = ctxt;
    }

    @Override
    public void enterCompositeTransform(TransformTreeNode node) {
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {
    }

    @Override
    public void visitTransform(TransformTreeNode node) {
      PTransform<?, ?> transform = node.getTransform();
      TransformEvaluator evaluator = EVALUATORS.get(transform.getClass());
      if (evaluator == null) {
        throw new IllegalStateException(
            "no evaluator registered for " + transform);
      }
      LOG.info("Evaluating " + transform);
      evaluator.evaluate(transform, ctxt);
    }

    @Override
    public void visitValue(PValue pvalue, TransformTreeNode node) {
    }
  }

  public static interface EvaluationResult extends PipelineResult {
  }

  public static class EvaluationContext implements EvaluationResult {
    final JavaSparkContext jsc;
    JavaRDDLike last;

    public EvaluationContext(String master) {
      this.jsc = new JavaSparkContext(master, "dataflow");
    }

    JavaSparkContext getSparkContext() {
      return jsc;
    }
    void setLast(JavaRDDLike rdd) {
      last = rdd;
    }

    JavaRDDLike getLast() { return last; }
  }

  public static interface TransformEvaluator<PT extends PTransform> extends Serializable {
    void evaluate(PT transform, EvaluationContext context);
  }

  private static class FieldGetter {
    private Map<String, Field> fields;

    public FieldGetter(Class<?> clazz) {
      this.fields = Maps.newHashMap();
      for (Field f : clazz.getDeclaredFields()) {
        try {
          f.setAccessible(true);
          this.fields.put(f.getName(), f);
          System.err.println("Field " + f.getName() + " for class = " + clazz);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    public <T> T get(String fieldname, Object value) {
      try {
        return (T) fields.get(fieldname).get(value);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static FieldGetter READ_TEXT_FG = new FieldGetter(TextIO.Read.Bound.class);
  private static FieldGetter WRITE_TEXT_FG = new FieldGetter(TextIO.Write.Bound.class);

  private static TransformEvaluator<TextIO.Read.Bound> READ_TEXT = new TransformEvaluator<TextIO.Read.Bound>() {
    @Override
    public void evaluate(TextIO.Read.Bound transform, EvaluationContext context) {
      String pattern = READ_TEXT_FG.get("filepattern", transform);
      JavaRDD rdd = context.getSparkContext().textFile(pattern);
      Coder coder = READ_TEXT_FG.get("coder", transform);
      if (coder != null) {
        //TODO
      }
      context.setLast(rdd);
    }
  };

  private static TransformEvaluator<TextIO.Write.Bound> WRITE_TEXT = new TransformEvaluator<TextIO.Write.Bound>() {
    @Override
    public void evaluate(TextIO.Write.Bound transform, EvaluationContext context) {
      JavaRDDLike last = context.getLast();
      Coder coder = WRITE_TEXT_FG.get("coder", transform);
      if (coder != null) {
        //TODO
      }
      String pattern = WRITE_TEXT_FG.get("filenamePrefix", transform);
      last.saveAsTextFile(pattern);
    }
  };

  private static FieldGetter CREATE_FG = new FieldGetter(Create.class);
  private static TransformEvaluator<Create> CREATE = new TransformEvaluator<Create>() {
    @Override
    public void evaluate(Create transform, EvaluationContext context) {
      Iterable elems = CREATE_FG.get("elems", transform);
      JavaRDD rdd = context.getSparkContext().parallelize(Lists.newLinkedList(elems));
      context.setLast(rdd);
    }
  };

  private static FieldGetter PARDO_FG = new FieldGetter(ParDo.Bound.class);
  private static TransformEvaluator<ParDo.Bound> PARDO = new TransformEvaluator<ParDo.Bound>() {
    @Override
    public void evaluate(ParDo.Bound transform, EvaluationContext context) {
      JavaRDDLike last = context.getLast();
      DoFnFunction dofn = new DoFnFunction(PARDO_FG.<DoFn>get("fn", transform));
      context.setLast(last.mapPartitions(dofn));
    }
  };

  private static TransformEvaluator<GroupByKey> GBK = new TransformEvaluator<GroupByKey>() {
    @Override
    public void evaluate(GroupByKey transform, EvaluationContext context) {
      context.setLast(fromPair(toPair(context.getLast()).groupByKey()));
    }

    private JavaPairRDD toPair(JavaRDDLike rdd) {
      return rdd.mapToPair(new PairFunction() {
        @Override
        public Tuple2 call(Object o) throws Exception {
          KV kv = (KV) o;
          return new Tuple2(kv.getKey(), kv.getValue());
        }
      });
    }

    private JavaRDDLike fromPair(JavaPairRDD rdd) {
      return rdd.map(new Function() {
        @Override
        public Object call(Object o) throws Exception {
          Tuple2 t2 = (Tuple2) o;
          return KV.of(t2._1(), t2._2());
        }
      });
    }
  };

  private static FieldGetter GROUPED_FG = new FieldGetter(Combine.GroupedValues.class);
  private static TransformEvaluator<Combine.GroupedValues> GROUPED = new TransformEvaluator<Combine.GroupedValues>() {
    @Override
    public void evaluate(Combine.GroupedValues transform, EvaluationContext context) {
      final Combine.KeyedCombineFn keyed = GROUPED_FG.get("fn", transform);
      context.setLast(context.getLast().map(new Function() {
        @Override
        public Object call(Object input) throws Exception {
          KV<Object, Iterable> kv = (KV<Object, Iterable>) input;
          return KV.of(kv.getKey(), keyed.apply(kv.getKey(), kv.getValue()));
        }
      }));
    }
  };

  private static final Map<Class, TransformEvaluator> EVALUATORS = ImmutableMap.<Class, TransformEvaluator>builder()
      .put(Combine.GroupedValues.class, GROUPED)
      .put(GroupByKey.class, GBK)
      .put(ParDo.Bound.class, PARDO)
      .put(TextIO.Read.Bound.class, READ_TEXT)
      .put(TextIO.Write.Bound.class, WRITE_TEXT)
      .put(Create.class, CREATE)
      .build();
}
