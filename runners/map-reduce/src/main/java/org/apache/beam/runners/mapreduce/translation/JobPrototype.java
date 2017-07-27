package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Created by peihe on 24/07/2017.
 */
public class JobPrototype {

  public static JobPrototype create(int stageId, Graph.Vertex vertex) {
    return new JobPrototype(stageId, vertex);
  }

  private final int stageId;
  private final Graph.Vertex vertex;
  private final Set<JobPrototype> dependencies;

  private JobPrototype(int stageId, Graph.Vertex vertex) {
    this.stageId = stageId;
    this.vertex = checkNotNull(vertex, "vertex");
    this.dependencies = Sets.newHashSet();
  }

  public Job build(Class<?> jarClass, Configuration conf) throws IOException {
    Job job = new Job(conf);
    conf = job.getConfiguration();
    job.setJarByClass(jarClass);
    conf.set(
        "io.serializations",
        "org.apache.hadoop.io.serializer.WritableSerialization," +
            "org.apache.hadoop.io.serializer.JavaSerialization");

    // Setup BoundedSources in BeamInputFormat.
    // TODO: support more than one in-edge
    Graph.Edge inEdge = Iterables.getOnlyElement(vertex.getIncoming());
    Graph.Vertex head = inEdge.getHead();
    Graph.Step headStep = head.getStep();
    checkState(headStep.getTransform() instanceof Read.Bounded);
    Read.Bounded read = (Read.Bounded) headStep.getTransform();
    conf.set(
        BeamInputFormat.BEAM_SERIALIZED_BOUNDED_SOURCE,
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(read.getSource())));
    job.setInputFormatClass(BeamInputFormat.class);

    // Setup DoFns in BeamMapper.
    // TODO: support more than one in-path.
    Graph.NodePath inPath = Iterables.getOnlyElement(inEdge.getPaths());

    Operation mapperParDoRoot = chainParDosInPath(inPath);
    Operation mapperParDoTail = getTailOperation(mapperParDoRoot);
    Graph.Step vertexStep = vertex.getStep();
    if (vertexStep.getTransform() instanceof ParDo.SingleOutput
        || vertexStep.getTransform() instanceof ParDo.MultiOutput
        || vertexStep.getTransform() instanceof Window.Assign) {
      // TODO: add a TailVertex type to simplify the translation.
      Operation vertexParDo = translateToOperation(vertexStep);
      Operation mapperWrite = new WriteOperation(
          getKeyCoder(inEdge.getCoder()),
          getReifyValueCoder(inEdge.getCoder(), vertexStep.getWindowingStrategy()));
      mapperParDoTail.attachOutput(vertexParDo, 0);
      vertexParDo.attachOutput(mapperWrite, 0);
    } else if (vertexStep.getTransform() instanceof GroupByKey) {
      Operation reifyOperation = new ReifyTimestampAndWindowsParDoOperation(
          PipelineOptionsFactory.create(),
          new TupleTag<>(),
          ImmutableList.<TupleTag<?>>of(),
          vertexStep.getWindowingStrategy());
      Operation mapperWrite = new WriteOperation(
          getKeyCoder(inEdge.getCoder()),
          getReifyValueCoder(inEdge.getCoder(), vertexStep.getWindowingStrategy()));
      mapperParDoTail.attachOutput(reifyOperation, 0);
      reifyOperation.attachOutput(mapperWrite, 0);
    } else {
      throw new UnsupportedOperationException("Transform: " + vertexStep.getTransform());
    }
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(byte[].class);
    conf.set(
        BeamMapper.BEAM_PAR_DO_OPERATION_MAPPER,
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(mapperParDoRoot)));
    job.setMapperClass(BeamMapper.class);

    if (vertexStep.getTransform() instanceof GroupByKey) {
      // Setup BeamReducer
      Operation gabwOperation = new GroupAlsoByWindowsParDoOperation(
          PipelineOptionsFactory.create(),
          (TupleTag<Object>) vertexStep.getOutputs().iterator().next(),
          ImmutableList.<TupleTag<?>>of(),
          vertexStep.getWindowingStrategy(),
          inEdge.getCoder());
      Graph.Edge outEdge = Iterables.getOnlyElement(vertex.getOutgoing());
      Graph.NodePath outPath = Iterables.getOnlyElement(outEdge.getPaths());
      Operation reducerParDoRoot = chainParDosInPath(outPath);
      Operation reducerParDoTail = getTailOperation(reducerParDoRoot);

      Operation reducerTailParDo = translateToOperation(outEdge.getTail().getStep());
      if (reducerParDoRoot == null) {
        gabwOperation.attachOutput(reducerTailParDo, 0);
      } else {
        gabwOperation.attachOutput(reducerParDoRoot, 0);
        reducerParDoTail.attachOutput(reducerTailParDo, 0);
      }
      conf.set(
          BeamReducer.BEAM_REDUCER_KV_CODER,
          Base64.encodeBase64String(SerializableUtils.serializeToByteArray(
              KvCoder.of(
                  getKeyCoder(inEdge.getCoder()),
                  getReifyValueCoder(inEdge.getCoder(), vertexStep.getWindowingStrategy())))));
      conf.set(
          BeamReducer.BEAM_PAR_DO_OPERATION_REDUCER,
          Base64.encodeBase64String(SerializableUtils.serializeToByteArray(gabwOperation)));
      job.setReducerClass(BeamReducer.class);
    }
    job.setOutputFormatClass(NullOutputFormat.class);
    return job;
  }

  private Coder<Object> getKeyCoder(Coder<?> coder) {
    KvCoder<Object, Object> kvCoder = (KvCoder<Object, Object>) checkNotNull(coder, "coder");
    return kvCoder.getKeyCoder();
  }

  private Coder<Object> getReifyValueCoder(
      Coder<?> coder, WindowingStrategy<?, ?> windowingStrategy) {
    KvCoder<Object, Object> kvCoder = (KvCoder<Object, Object>) checkNotNull(coder, "coder");
    return (Coder) WindowedValue.getFullCoder(
        kvCoder.getValueCoder(), windowingStrategy.getWindowFn().windowCoder());
  }

  private Operation getTailOperation(@Nullable Operation operation) {
    if (operation == null) {
      return null;
    }
    if (operation.getOutputReceivers().isEmpty()) {
      return operation;
    }
    OutputReceiver receiver = Iterables.getOnlyElement(operation.getOutputReceivers());
    if (receiver.getReceivingOperations().isEmpty()) {
      return operation;
    }
    return getTailOperation(Iterables.getOnlyElement(receiver.getReceivingOperations()));
  }

  private Operation chainParDosInPath(Graph.NodePath path) {
    List<Graph.Step> parDos = new ArrayList<>();
    // TODO: we should not need this filter.
    parDos.addAll(FluentIterable.from(path.steps())
        .filter(new Predicate<Graph.Step>() {
          @Override
          public boolean apply(Graph.Step input) {
            PTransform<?, ?> transform = input.getTransform();
            return !(transform instanceof Read.Bounded);
          }})
        .toList());

    Operation root = null;
    Operation prev = null;
    for (Graph.Step step : parDos) {
      Operation current = translateToOperation(step);
      if (prev == null) {
        root = current;
      } else {
        // TODO: set a proper outputNum for ParDo.MultiOutput instead of zero.
        prev.attachOutput(current, 0);
      }
      prev = current;
    }
    return root;
  }

  private Operation translateToOperation(Graph.Step parDoStep) {
    PTransform<?, ?> transform = parDoStep.getTransform();
    DoFn<Object, Object> doFn;
    if (transform instanceof ParDo.SingleOutput) {
      return new NormalParDoOperation(
          ((ParDo.SingleOutput) transform).getFn(),
          PipelineOptionsFactory.create(),
          (TupleTag<Object>) parDoStep.getOutputs().iterator().next(),
          ImmutableList.<TupleTag<?>>of(),
          parDoStep.getWindowingStrategy());
    } else if (transform instanceof ParDo.MultiOutput) {
      return new NormalParDoOperation(
          ((ParDo.MultiOutput) transform).getFn(),
          PipelineOptionsFactory.create(),
          (TupleTag<Object>) parDoStep.getOutputs().iterator().next(),
          ImmutableList.<TupleTag<?>>of(),
          parDoStep.getWindowingStrategy());
    } else if (transform instanceof Window.Assign) {
      return new WindowAssignOperation<>(1, parDoStep.getWindowingStrategy().getWindowFn());
    } else {
      throw new UnsupportedOperationException("Transform: " + transform);
    }
  }
}
