package org.apache.beam.runners.mapreduce.translation;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link GraphConverter}.
 */
@RunWith(JUnit4.class)
public class GraphConverterTest {

  @Test
  public void testCombine() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<KV<String, Integer>> input = p
        .apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
        .apply(Sum.<String>integersPerKey());
    GraphConverter graphConverter = new GraphConverter();
    p.traverseTopologically(graphConverter);

    Graph graph = graphConverter.getGraph();

    assertEquals(3, Iterables.size(graph.getAllVertices()));
    assertEquals(2, Iterables.size(graph.getAllEdges()));
    assertEquals(1, Iterables.size(graph.getLeafVertices()));
  }
}
