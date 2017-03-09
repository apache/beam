package org.beam.sdk.java.sql.rel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.beam.sdk.java.sql.planner.BeamPipelineCreator;

public class BeamIOSourceRel extends TableScan implements BeamRelNode {

  public BeamIOSourceRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, table);
  }

  @Override
  public void buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception {
    // TODO Auto-generated method stub
    
  }

}
