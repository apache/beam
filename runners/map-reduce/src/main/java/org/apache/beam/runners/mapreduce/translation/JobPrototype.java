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
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TupleTag;
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
    List<Graph.Step> parDos = new ArrayList<>();
    parDos.addAll(FluentIterable.from(inPath.steps())
        .filter(new Predicate<Graph.Step>() {
          @Override
          public boolean apply(Graph.Step input) {
            PTransform<?, ?> transform = input.getTransform();
            return transform instanceof ParDo.SingleOutput
                || transform instanceof ParDo.MultiOutput;
          }})
        .toList());
    Graph.Step vertexStep = vertex.getStep();
    if (vertexStep.getTransform() instanceof ParDo.SingleOutput
        || vertexStep.getTransform() instanceof ParDo.MultiOutput) {
      parDos.add(vertexStep);
    }

    ParDoOperation root = null;
    ParDoOperation prev = null;
    for (Graph.Step step : parDos) {
      ParDoOperation current = new NormalParDoOperation(
          getDoFn(step.getTransform()),
          PipelineOptionsFactory.create(),
          (TupleTag<Object>) step.getOutputs().iterator().next(),
          ImmutableList.<TupleTag<?>>of(),
          step.getWindowingStrategy());
      if (root == null) {
        root = current;
      } else {
        // TODO: set a proper outputNum for ParDo.MultiOutput instead of zero.
        current.attachInput(prev, 0);
      }
      prev = current;
    }
    // TODO: get coders from pipeline.
    WriteOperation writeOperation = new WriteOperation(inEdge.getCoder());
    writeOperation.attachInput(prev, 0);
    conf.set(
        BeamMapper.BEAM_PAR_DO_OPERATION_MAPPER,
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(root)));
    job.setMapperClass(BeamMapper.class);

    if (vertexStep.getTransform() instanceof GroupByKey) {
      // Setup BeamReducer
      ParDoOperation operation = new GroupAlsoByWindowsParDoOperation(
          PipelineOptionsFactory.create(),
          (TupleTag<Object>) vertexStep.getOutputs().iterator().next(),
          ImmutableList.<TupleTag<?>>of(),
          vertexStep.getWindowingStrategy(),
          inEdge.getCoder());
      // TODO: handle the map output key type.
      job.setMapOutputKeyClass(BytesWritable.class);
      job.setMapOutputValueClass(byte[].class);
      conf.set(
          BeamReducer.BEAM_PAR_DO_OPERATION_REDUCER,
          Base64.encodeBase64String(SerializableUtils.serializeToByteArray(operation)));
      job.setReducerClass(BeamReducer.class);
    }
    job.setOutputFormatClass(NullOutputFormat.class);
    return job;
  }

  private DoFn<Object, Object> getDoFn(PTransform<?, ?> transform) {
    if (transform instanceof ParDo.SingleOutput) {
      return ((ParDo.SingleOutput) transform).getFn();
    } else {
      return ((ParDo.MultiOutput) transform).getFn();
    }
  }
}
