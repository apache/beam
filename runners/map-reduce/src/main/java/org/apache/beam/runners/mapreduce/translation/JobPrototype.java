package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
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

    // Setup BoundedSources in BeamInputFormat.
    // TODO: support more than one inputs
    Graph.Vertex head = Iterables.getOnlyElement(vertex.getIncoming()).getHead();
    checkState(head.getTransform() instanceof Read.Bounded);
    Read.Bounded read = (Read.Bounded) head.getTransform();
    conf.set(
        BeamInputFormat.BEAM_SERIALIZED_BOUNDED_SOURCE,
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(read.getSource())));
    job.setInputFormatClass(BeamInputFormat.class);

    // Setup DoFns in BeamMapper.
    // TODO: support more than one out going edge.
    Graph.Edge outEdge = Iterables.getOnlyElement(head.getOutgoing());
    Graph.NodePath outPath = Iterables.getOnlyElement(outEdge.getPaths());
    List<DoFn> doFns = new ArrayList<>();
    doFns.addAll(FluentIterable.from(outPath.transforms())
        .filter(new Predicate<PTransform<?, ?>>() {
          @Override
          public boolean apply(PTransform<?, ?> input) {
            return !(input instanceof Read.Bounded);
          }
        })
        .transform(new Function<PTransform<?, ?>, DoFn>() {
          @Override
          public DoFn apply(PTransform<?, ?> input) {
            checkArgument(
                input instanceof ParDo.SingleOutput, "Only support ParDo.SingleOutput.");
            ParDo.SingleOutput parDo = (ParDo.SingleOutput) input;
            return parDo.getFn();
          }})
        .toList());
    if (vertex.getTransform() instanceof ParDo.SingleOutput) {
      doFns.add(((ParDo.SingleOutput) vertex.getTransform()).getFn());
    } else if (vertex.getTransform() instanceof ParDo.MultiOutput) {
      doFns.add(((ParDo.MultiOutput) vertex.getTransform()).getFn());
    }
    conf.set(
        BeamMapper.BEAM_SERIALIZED_DO_FN,
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(
            Iterables.getOnlyElement(doFns))));
    job.setMapperClass(BeamMapper.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    return job;
  }
}
