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

import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TypedPValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

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
    EvaluationContext ctxt = new EvaluationContext(this.master, pipeline);
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

  private static TransformEvaluator<TextIO.Read.Bound> READ_TEXT = new TransformEvaluator<TextIO.Read.Bound>() {
    @Override
    public void evaluate(TextIO.Read.Bound transform, EvaluationContext context) {
      String pattern = transform.getFilepattern();
      JavaRDD rdd = context.getSparkContext().textFile(pattern);
      // TODO: handle coders
      context.setOutputRDD(transform, rdd);
    }
  };

  private static TransformEvaluator<TextIO.Write.Bound> WRITE_TEXT = new TransformEvaluator<TextIO.Write.Bound>() {
    @Override
    public void evaluate(TextIO.Write.Bound transform, EvaluationContext context) {
      JavaRDDLike last = context.getInputRDD(transform);
      // TODO: handle coders
      String pattern = transform.getFilenamePrefix();
      last.saveAsTextFile(pattern);
    }
  };

  private static TransformEvaluator<AvroIO.Read.Bound> READ_AVRO = new TransformEvaluator<AvroIO.Read.Bound>() {
    @Override
    public void evaluate(AvroIO.Read.Bound transform, EvaluationContext context) {
      String pattern = transform.getFilepattern();
      JavaRDD rdd = context.getSparkContext().textFile(pattern);
      // TODO: handle coders
      context.setOutputRDD(transform, rdd);
    }
  };

  private static TransformEvaluator<AvroIO.Write.Bound> WRITE_AVRO = new TransformEvaluator<AvroIO.Write.Bound>() {
    @Override
    public void evaluate(AvroIO.Write.Bound transform, EvaluationContext context) {
      JavaRDDLike last = context.getInputRDD(transform);
      Coder coder = null;
      if (coder != null) {
        //TODO
      }
      String pattern = transform.getFilenamePrefix();
      last.saveAsTextFile(pattern);
    }
  };

  private static TransformEvaluator<Create> CREATE = new TransformEvaluator<Create>() {
    @Override
    public void evaluate(Create transform, EvaluationContext context) {
      Iterable elems = transform.getElements();
      Coder coder = ((PCollection) context.getOutput(transform)).getCoder();
      JavaRDD rdd = context.getSparkContext().parallelize(
          CoderHelpers.toByteArrays(elems, coder));
      context.setOutputRDD(transform, rdd.map(CoderHelpers.fromByteFunction(coder)));
    }
  };

  private static TransformEvaluator<CreatePObject> CREATE_POBJ = new TransformEvaluator<CreatePObject>() {
    @Override
    public void evaluate(CreatePObject transform, EvaluationContext context) {
      context.setPObjectValue((PObject) context.getOutput(transform), transform.getElement());
    }
  };

  private static TransformEvaluator<ParDo.Bound> PARDO = new TransformEvaluator<ParDo.Bound>() {
    @Override
    public void evaluate(ParDo.Bound transform, EvaluationContext context) {
      JavaRDDLike last = context.getInputRDD(transform);
      PObjectTuple pot = transform.getSideInputs();
      DoFnFunction dofn;
      if (pot == null || pot.getAll().isEmpty()) {
        dofn = new DoFnFunction(transform.getFn());
      } else {
        Map<TupleTag<?>, BroadcastHelper<?>> sideInputs = Maps.newHashMap();
        for (Map.Entry<TupleTag<?>, PObject<?>> e : pot.getAll().entrySet()) {
          sideInputs.put(e.getKey(), context.getBroadcastHelper(e.getValue()));
        }
        dofn = new DoFnFunction(transform.getFn(), sideInputs);
      }
      context.setOutputRDD(transform, last.mapPartitions(dofn));
    }
  };

  private static JavaPairRDD toPair(JavaRDDLike rdd) {
    return rdd.mapToPair(new PairFunction() {
      @Override
      public Tuple2 call(Object o) throws Exception {
        KV kv = (KV) o;
        return new Tuple2(kv.getKey(), kv.getValue());
      }
    });
  }

  private static JavaRDDLike fromPair(JavaPairRDD rdd) {
    return rdd.map(new Function() {
      @Override
      public Object call(Object o) throws Exception {
        Tuple2 t2 = (Tuple2) o;
        return KV.of(t2._1(), t2._2());
      }
    });
  }

  private static TransformEvaluator<GroupByKey> GBK = new TransformEvaluator<GroupByKey>() {
    @Override
    public void evaluate(GroupByKey transform, EvaluationContext context) {
      context.setOutputRDD(transform, fromPair(toPair(context.getInputRDD(transform)).groupByKey()));
    }
  };

  private static TransformEvaluator<Combine.GroupedValues> GROUPED = new TransformEvaluator<Combine.GroupedValues>() {
    @Override
    public void evaluate(Combine.GroupedValues transform, EvaluationContext context) {
      final Combine.KeyedCombineFn keyed = transform.getFn();
      context.setOutputRDD(transform, context.getInputRDD(transform).map(new Function() {
        @Override
        public Object call(Object input) throws Exception {
          KV<Object, Iterable> kv = (KV<Object, Iterable>) input;
          return KV.of(kv.getKey(), keyed.apply(kv.getKey(), kv.getValue()));
        }
      }));
    }
  };

  private static TransformEvaluator<Flatten> FLATTEN = new TransformEvaluator<Flatten>() {
    @Override
    public void evaluate(Flatten transform, EvaluationContext context) {
      PCollectionList<?> pcs = (PCollectionList<?>) context.getPipeline().getInput(transform);
      JavaRDD[] rdds = new JavaRDD[pcs.size()];
      for (int i = 0; i < rdds.length; i++) {
        rdds[i] = (JavaRDD) context.getRDD(pcs.get(i));
      }
      JavaRDD rdd = context.getSparkContext().union(rdds);
      context.setOutputRDD(transform, rdd);
    }
  };

  public static <PT extends PTransform> void registerEvaluator(
      Class<PT> transformClass,
      TransformEvaluator<PT> evaluator) {
    EVALUATORS.put(transformClass, evaluator);
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator> EVALUATORS = Maps.newHashMap();
  static {
    registerEvaluator(TextIO.Read.Bound.class, READ_TEXT);
    registerEvaluator(TextIO.Write.Bound.class, WRITE_TEXT);
    registerEvaluator(AvroIO.Read.Bound.class, READ_AVRO);
    registerEvaluator(AvroIO.Write.Bound.class, WRITE_AVRO);
    registerEvaluator(ParDo.Bound.class, PARDO);
    registerEvaluator(GroupByKey.class, GBK);
    registerEvaluator(Combine.GroupedValues.class, GROUPED);
    registerEvaluator(Flatten.class, FLATTEN);
    registerEvaluator(Create.class, CREATE);
    registerEvaluator(CreatePObject.class, CREATE_POBJ);
  }
}
