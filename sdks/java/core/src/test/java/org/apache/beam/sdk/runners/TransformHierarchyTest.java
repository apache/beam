/*
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
package org.apache.beam.sdk.runners;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.Defaults;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TransformHierarchy}. */
@RunWith(JUnit4.class)
public class TransformHierarchyTest implements Serializable {
  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private transient TransformHierarchy hierarchy;

  @Before
  public void setup() {
    hierarchy = new TransformHierarchy(ResourceHints.create());
  }

  @Test
  public void getCurrentNoPushReturnsRoot() {
    assertThat(hierarchy.getCurrent().isRootNode(), is(true));
  }

  @Test
  public void pushWithoutPushFails() {
    thrown.expect(IllegalStateException.class);
    hierarchy.popNode();
  }

  @Test
  public void pushThenPopSucceeds() {
    TransformHierarchy.Node root = hierarchy.getCurrent();
    TransformHierarchy.Node node = hierarchy.pushNode("Create", PBegin.in(pipeline), Create.of(1));
    assertThat(hierarchy.getCurrent(), equalTo(node));
    hierarchy.popNode();
    assertThat(node.finishedSpecifying, is(true));
    assertThat(hierarchy.getCurrent(), equalTo(root));
  }

  @Test
  public void emptyCompositeSucceeds() {
    PCollection<Long> created =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarLongCoder.of());
    TransformHierarchy.Node node = hierarchy.pushNode("Create", PBegin.in(pipeline), Create.of(1));
    hierarchy.setOutput(created);
    hierarchy.popNode();
    PCollectionList<Long> pcList = PCollectionList.of(created);

    TransformHierarchy.Node emptyTransform =
        hierarchy.pushNode(
            "Extract",
            pcList,
            new PTransform<PCollectionList<Long>, PCollection<Long>>() {
              @Override
              public PCollection<Long> expand(PCollectionList<Long> input) {
                return input.get(0);
              }
            });
    hierarchy.setOutput(created);
    hierarchy.popNode();
    assertThat(hierarchy.getProducer(created), equalTo(node));
    assertThat(
        "A Transform that produces non-primitive output should be composite",
        emptyTransform.isCompositeNode(),
        is(true));
  }

  @Test
  public void producingOwnAndOthersOutputsFails() {
    PCollection<Long> created =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarLongCoder.of());
    hierarchy.pushNode("Create", PBegin.in(pipeline), Create.of(1));
    hierarchy.setOutput(created);
    hierarchy.popNode();
    PCollectionList<Long> pcList = PCollectionList.of(created);

    final PCollectionList<Long> appended =
        pcList.and(
            PCollection.createPrimitiveOutputInternal(
                    pipeline,
                    WindowingStrategy.globalDefault(),
                    IsBounded.BOUNDED,
                    VarLongCoder.of())
                .setName("prim"));
    hierarchy.pushNode(
        "AddPc",
        pcList,
        new PTransform<PCollectionList<Long>, PCollectionList<Long>>() {
          @Override
          public PCollectionList<Long> expand(PCollectionList<Long> input) {
            return appended;
          }
        });
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("contains a PCollection produced by it");
    thrown.expectMessage("AddPc");
    thrown.expectMessage("Create");
    thrown.expectMessage(appended.expand().toString());
    hierarchy.setOutput(appended);
  }

  @Test
  public void producingOwnOutputWithCompositeFails() {
    final PCollection<Long> comp =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarLongCoder.of());
    PTransform<PBegin, PCollection<Long>> root =
        new PTransform<PBegin, PCollection<Long>>() {
          @Override
          public PCollection<Long> expand(PBegin input) {
            return comp;
          }
        };
    hierarchy.pushNode("Composite", PBegin.in(pipeline), root);

    Create.Values<Integer> create = Create.of(1);
    hierarchy.pushNode("Create", PBegin.in(pipeline), create);
    hierarchy.setOutput(pipeline.apply(create));
    hierarchy.popNode();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("contains a PCollection produced by it");
    thrown.expectMessage("primitive transforms are permitted to produce");
    thrown.expectMessage("Composite");
    hierarchy.setOutput(comp);
  }

  @Test
  public void replaceSucceeds() {
    PTransform<?, ?> enclosingPT =
        new PTransform<PInput, POutput>() {
          @Override
          public POutput expand(PInput input) {
            return PDone.in(input.getPipeline());
          }
        };

    TransformHierarchy.Node enclosing =
        hierarchy.pushNode("Enclosing", PBegin.in(pipeline), enclosingPT);

    Create.Values<Long> originalTransform = Create.of(1L);
    TransformHierarchy.Node original =
        hierarchy.pushNode("Create", PBegin.in(pipeline), originalTransform);
    assertThat(hierarchy.getCurrent(), equalTo(original));
    PCollection<Long> originalOutput = pipeline.apply(originalTransform);
    hierarchy.setOutput(originalOutput);
    hierarchy.popNode();
    assertThat(original.finishedSpecifying, is(true));
    hierarchy.setOutput(PDone.in(pipeline));
    hierarchy.popNode();

    assertThat(hierarchy.getCurrent(), not(equalTo(enclosing)));
    Read.Bounded<Long> replacementTransform = Read.from(CountingSource.upTo(1L));
    PCollection<Long> replacementOutput = pipeline.apply(replacementTransform);
    Node replacement = hierarchy.replaceNode(original, PBegin.in(pipeline), replacementTransform);
    assertThat(hierarchy.getCurrent(), equalTo(replacement));
    hierarchy.setOutput(replacementOutput);

    TaggedPValue taggedReplacement = TaggedPValue.ofExpandedValue(replacementOutput);
    Map<PCollection<?>, ReplacementOutput> replacementOutputs =
        Collections.singletonMap(
            replacementOutput,
            ReplacementOutput.of(TaggedPValue.ofExpandedValue(originalOutput), taggedReplacement));
    hierarchy.replaceOutputs(replacementOutputs);

    assertThat(replacement.getInputs(), equalTo(original.getInputs()));
    assertThat(replacement.getEnclosingNode(), equalTo(original.getEnclosingNode()));
    assertThat(replacement.getEnclosingNode(), equalTo(enclosing));
    assertThat(replacement.getTransform(), equalTo(replacementTransform));
    // THe tags of the replacement transform are matched to the appropriate PValues of the original
    assertThat(replacement.getOutputs().keySet(), Matchers.contains(taggedReplacement.getTag()));
    assertThat(replacement.getOutputs().values(), Matchers.contains(originalOutput));
    hierarchy.popNode();
  }

  @Test
  public void replaceWithCompositeSucceeds() {
    final SingleOutput<Long, Long> originalParDo =
        ParDo.of(
            new DoFn<Long, Long>() {
              @ProcessElement
              public void processElement(ProcessContext ctxt) {
                ctxt.output(ctxt.element() + 1L);
              }
            });

    GenerateSequence genUpstream = GenerateSequence.from(0);
    PCollection<Long> upstream = pipeline.apply(genUpstream);
    PCollection<Long> output = upstream.apply("Original", originalParDo);
    hierarchy.pushNode("Upstream", pipeline.begin(), genUpstream);
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(upstream);
    hierarchy.popNode();

    TransformHierarchy.Node original = hierarchy.pushNode("Original", upstream, originalParDo);
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(output);
    hierarchy.popNode();

    final TupleTag<Long> longs = new TupleTag<>();
    final MultiOutput<Long, Long> replacementParDo =
        ParDo.of(
                new DoFn<Long, Long>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctxt) {
                    ctxt.output(ctxt.element() + 1L);
                  }
                })
            .withOutputTags(longs, TupleTagList.empty());
    PTransform<PCollection<Long>, PCollection<Long>> replacementComposite =
        new PTransform<PCollection<Long>, PCollection<Long>>() {
          @Override
          public PCollection<Long> expand(PCollection<Long> input) {
            return input.apply("Contained", replacementParDo).get(longs);
          }
        };

    PCollectionTuple replacementOutput = upstream.apply("Contained", replacementParDo);

    Node compositeNode = hierarchy.replaceNode(original, upstream, replacementComposite);
    Node replacementParNode = hierarchy.pushNode("Original/Contained", upstream, replacementParDo);
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(replacementOutput);
    hierarchy.popNode();
    hierarchy.setOutput(replacementOutput.get(longs));

    Entry<TupleTag<?>, PCollection<?>> replacementLongs =
        (Map.Entry) Iterables.getOnlyElement(replacementOutput.expand().entrySet());
    hierarchy.replaceOutputs(
        Collections.singletonMap(
            replacementOutput.get(longs),
            ReplacementOutput.of(
                TaggedPValue.ofExpandedValue(output),
                TaggedPValue.of(replacementLongs.getKey(), replacementLongs.getValue()))));

    assertThat(
        replacementParNode.getOutputs().keySet(), Matchers.contains(replacementLongs.getKey()));
    assertThat(replacementParNode.getOutputs().values(), Matchers.contains(output));
    assertThat(
        compositeNode.getOutputs().keySet(),
        equalTo(replacementOutput.get(longs).expand().keySet()));
    assertThat(compositeNode.getOutputs().values(), Matchers.contains(output));
    hierarchy.popNode();
  }

  @Test
  public void visitVisitsAllPushed() {
    TransformHierarchy.Node root = hierarchy.getCurrent();
    PBegin begin = PBegin.in(pipeline);

    Create.Values<Long> create = Create.of(1L);
    Read.Bounded<Long> read = Read.from(CountingSource.upTo(1L));

    PCollection<Long> created =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarLongCoder.of());

    SingleOutput<Long, Long> pardo =
        ParDo.of(
            new DoFn<Long, Long>() {
              @ProcessElement
              public void processElement(ProcessContext ctxt) {
                ctxt.output(ctxt.element());
              }
            });

    PCollection<Long> mapped =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarLongCoder.of());

    TransformHierarchy.Node compositeNode = hierarchy.pushNode("Create", begin, create);
    hierarchy.finishSpecifyingInput();
    assertThat(hierarchy.getCurrent(), equalTo(compositeNode));
    assertThat(compositeNode.getInputs().entrySet(), Matchers.empty());
    assertThat(compositeNode.getTransform(), equalTo(create));
    // Not yet set
    assertThat(compositeNode.getOutputs().entrySet(), Matchers.emptyIterable());
    assertThat(compositeNode.getEnclosingNode().isRootNode(), is(true));

    TransformHierarchy.Node primitiveNode = hierarchy.pushNode("Create/Read", begin, read);
    assertThat(hierarchy.getCurrent(), equalTo(primitiveNode));
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(created);
    hierarchy.popNode();
    assertThat(primitiveNode.getOutputs().values(), containsInAnyOrder(created));
    assertThat(primitiveNode.getInputs().entrySet(), Matchers.emptyIterable());
    assertThat(primitiveNode.getTransform(), equalTo(read));
    assertThat(primitiveNode.getEnclosingNode(), equalTo(compositeNode));

    hierarchy.setOutput(created);
    // The composite is listed as outputting a PValue created by the contained primitive
    assertThat(compositeNode.getOutputs().values(), containsInAnyOrder(created));
    // The producer of that PValue is still the primitive in which it is first output
    assertThat(hierarchy.getProducer(created), equalTo(primitiveNode));
    hierarchy.popNode();

    TransformHierarchy.Node otherPrimitive = hierarchy.pushNode("ParDo", created, pardo);
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(mapped);
    hierarchy.popNode();

    final Set<TransformHierarchy.Node> visitedCompositeNodes = new HashSet<>();
    final Set<TransformHierarchy.Node> visitedPrimitiveNodes = new HashSet<>();
    final Set<PValue> visitedValuesInVisitor = new HashSet<>();

    Set<PValue> visitedValues =
        hierarchy.visit(
            new PipelineVisitor.Defaults() {
              @Override
              public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
                visitedCompositeNodes.add(node);
                return CompositeBehavior.ENTER_TRANSFORM;
              }

              @Override
              public void visitPrimitiveTransform(TransformHierarchy.Node node) {
                visitedPrimitiveNodes.add(node);
              }

              @Override
              public void visitValue(PValue value, TransformHierarchy.Node producer) {
                visitedValuesInVisitor.add(value);
              }
            });

    assertThat(visitedCompositeNodes, containsInAnyOrder(root, compositeNode));
    assertThat(visitedPrimitiveNodes, containsInAnyOrder(primitiveNode, otherPrimitive));
    assertThat(visitedValuesInVisitor, containsInAnyOrder(created, mapped));
    assertThat(visitedValuesInVisitor, equalTo(visitedValues));
  }

  /**
   * Tests that visiting the {@link TransformHierarchy} after replacing nodes does not visit any of
   * the original nodes or inaccessible values but does visit all of the replacement nodes, new
   * inaccessible replacement values, and the original output values.
   */
  @Test
  public void visitAfterReplace() {
    Node root = hierarchy.getCurrent();
    final SingleOutput<Long, Long> originalParDo =
        ParDo.of(
            new DoFn<Long, Long>() {
              @ProcessElement
              public void processElement(ProcessContext ctxt) {
                ctxt.output(ctxt.element() + 1L);
              }
            });

    GenerateSequence genUpstream = GenerateSequence.from(0);
    PCollection<Long> upstream = pipeline.apply(genUpstream);
    PCollection<Long> output = upstream.apply("Original", originalParDo);
    Node upstreamNode = hierarchy.pushNode("Upstream", pipeline.begin(), genUpstream);
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(upstream);
    hierarchy.popNode();

    Node original = hierarchy.pushNode("Original", upstream, originalParDo);
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(output);
    hierarchy.popNode();

    final TupleTag<Long> longs = new TupleTag<>();
    final MultiOutput<Long, Long> replacementParDo =
        ParDo.of(
                new DoFn<Long, Long>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctxt) {
                    ctxt.output(ctxt.element() + 1L);
                  }
                })
            .withOutputTags(longs, TupleTagList.empty());
    PTransform<PCollection<Long>, PCollection<Long>> replacementComposite =
        new PTransform<PCollection<Long>, PCollection<Long>>() {
          @Override
          public PCollection<Long> expand(PCollection<Long> input) {
            return input.apply("Contained", replacementParDo).get(longs);
          }
        };

    PCollectionTuple replacementOutput = upstream.apply("Contained", replacementParDo);

    Node compositeNode = hierarchy.replaceNode(original, upstream, replacementComposite);
    Node replacementParNode = hierarchy.pushNode("Original/Contained", upstream, replacementParDo);
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(replacementOutput);
    hierarchy.popNode();
    hierarchy.setOutput(replacementOutput.get(longs));

    Map<TupleTag<?>, PCollection<?>> expandedReplacementOutput = (Map) replacementOutput.expand();
    Entry<TupleTag<?>, PCollection<?>> replacementLongs =
        Iterables.getOnlyElement(expandedReplacementOutput.entrySet());
    hierarchy.replaceOutputs(
        Collections.singletonMap(
            replacementOutput.get(longs),
            ReplacementOutput.of(
                TaggedPValue.ofExpandedValue(output),
                TaggedPValue.of(replacementLongs.getKey(), replacementLongs.getValue()))));
    hierarchy.popNode();

    final Set<Node> visitedCompositeNodes = new HashSet<>();
    final Set<Node> visitedPrimitiveNodes = new HashSet<>();
    Set<PValue> visitedValues =
        hierarchy.visit(
            new Defaults() {
              @Override
              public CompositeBehavior enterCompositeTransform(Node node) {
                visitedCompositeNodes.add(node);
                return CompositeBehavior.ENTER_TRANSFORM;
              }

              @Override
              public void visitPrimitiveTransform(Node node) {
                visitedPrimitiveNodes.add(node);
              }
            });

    /*
    Final Graph:
    Upstream -> Upstream.out -> Composite -> (ReplacementParDo -> OriginalParDo.out)
    */
    assertThat(visitedCompositeNodes, containsInAnyOrder(root, compositeNode));
    assertThat(visitedPrimitiveNodes, containsInAnyOrder(upstreamNode, replacementParNode));
    assertThat(visitedValues, containsInAnyOrder(upstream, output));
  }

  @Test
  public void visitIsTopologicallyOrdered() {
    PCollection<String> one =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, StringUtf8Coder.of());
    final PCollection<Integer> two =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED, VarIntCoder.of());
    final PDone done = PDone.in(pipeline);
    final TupleTag<String> oneTag = new TupleTag<String>() {};
    final TupleTag<Integer> twoTag = new TupleTag<Integer>() {};
    final PCollectionTuple oneAndTwo = PCollectionTuple.of(oneTag, one).and(twoTag, two);

    PTransform<PCollection<String>, PDone> multiConsumer =
        new PTransform<PCollection<String>, PDone>() {
          @Override
          public PDone expand(PCollection<String> input) {
            return done;
          }

          @Override
          public Map<TupleTag<?>, PValue> getAdditionalInputs() {
            return Collections.singletonMap(twoTag, two);
          }
        };
    hierarchy.pushNode("consumes_both", one, multiConsumer);
    hierarchy.setOutput(done);
    hierarchy.popNode();

    final PTransform<PBegin, PCollectionTuple> producer =
        new PTransform<PBegin, PCollectionTuple>() {
          @Override
          public PCollectionTuple expand(PBegin input) {
            return oneAndTwo;
          }
        };
    hierarchy.pushNode(
        "encloses_producer",
        PBegin.in(pipeline),
        new PTransform<PBegin, PCollectionTuple>() {
          @Override
          public PCollectionTuple expand(PBegin input) {
            return input.apply(producer);
          }
        });
    hierarchy.pushNode("creates_one_and_two", PBegin.in(pipeline), producer);
    hierarchy.setOutput(oneAndTwo);
    hierarchy.popNode();
    hierarchy.setOutput(oneAndTwo);
    hierarchy.popNode();

    hierarchy.pushNode("second_copy_of_consumes_both", one, multiConsumer);
    hierarchy.setOutput(done);
    hierarchy.popNode();

    final Set<Node> visitedNodes = new HashSet<>();
    final Set<Node> exitedNodes = new HashSet<>();
    final Set<PValue> visitedValues = new HashSet<>();
    hierarchy.visit(
        new PipelineVisitor.Defaults() {

          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            for (PValue input : node.getInputs().values()) {
              assertThat(visitedValues, hasItem(input));
            }
            assertThat(
                "Nodes should not be visited more than once", visitedNodes, not(hasItem(node)));
            if (!node.isRootNode()) {
              assertThat(
                  "Nodes should always be visited after their enclosing nodes",
                  visitedNodes,
                  hasItem(node.getEnclosingNode()));
            }
            visitedNodes.add(node);
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void leaveCompositeTransform(Node node) {
            assertThat(visitedNodes, hasItem(node));
            if (!node.isRootNode()) {
              assertThat(
                  "Nodes should always be left before their enclosing nodes are left",
                  exitedNodes,
                  not(hasItem(node.getEnclosingNode())));
            }
            assertThat(exitedNodes, not(hasItem(node)));
            exitedNodes.add(node);
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            assertThat(visitedNodes, hasItem(node.getEnclosingNode()));
            assertThat(exitedNodes, not(hasItem(node.getEnclosingNode())));
            assertThat(
                "Nodes should not be visited more than once", visitedNodes, not(hasItem(node)));
            for (PValue input : node.getInputs().values()) {
              assertThat(visitedValues, hasItem(input));
            }
            visitedNodes.add(node);
          }

          @Override
          public void visitValue(PValue value, Node producer) {
            assertThat(visitedNodes, hasItem(producer));
            assertThat(visitedValues, not(hasItem(value)));
            visitedValues.add(value);
          }
        });
    assertThat("Should have visited all the nodes", visitedNodes.size(), equalTo(5));
    assertThat("Should have left all of the visited composites", exitedNodes.size(), equalTo(2));
  }

  @Test
  public void visitDoesNotVisitSkippedNodes() {
    PCollection<String> one =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, StringUtf8Coder.of());
    final PCollection<Integer> two =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED, VarIntCoder.of());
    final PDone done = PDone.in(pipeline);
    final TupleTag<String> oneTag = new TupleTag<String>() {};
    final TupleTag<Integer> twoTag = new TupleTag<Integer>() {};
    final PCollectionTuple oneAndTwo = PCollectionTuple.of(oneTag, one).and(twoTag, two);

    hierarchy.pushNode(
        "consumes_both",
        one,
        new PTransform<PCollection<String>, PDone>() {
          @Override
          public PDone expand(PCollection<String> input) {
            return done;
          }

          @Override
          public Map<TupleTag<?>, PValue> getAdditionalInputs() {
            return Collections.singletonMap(twoTag, two);
          }
        });
    hierarchy.setOutput(done);
    hierarchy.popNode();

    final PTransform<PBegin, PCollectionTuple> producer =
        new PTransform<PBegin, PCollectionTuple>() {
          @Override
          public PCollectionTuple expand(PBegin input) {
            return oneAndTwo;
          }
        };
    final Node enclosing =
        hierarchy.pushNode(
            "encloses_producer",
            PBegin.in(pipeline),
            new PTransform<PBegin, PCollectionTuple>() {
              @Override
              public PCollectionTuple expand(PBegin input) {
                return input.apply(producer);
              }
            });
    Node enclosed = hierarchy.pushNode("creates_one_and_two", PBegin.in(pipeline), producer);
    hierarchy.setOutput(oneAndTwo);
    hierarchy.popNode();
    hierarchy.setOutput(oneAndTwo);
    hierarchy.popNode();

    final Set<Node> visitedNodes = new HashSet<>();
    hierarchy.visit(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            visitedNodes.add(node);
            return node.equals(enclosing)
                ? CompositeBehavior.DO_NOT_ENTER_TRANSFORM
                : CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            visitedNodes.add(node);
          }
        });

    assertThat(visitedNodes, hasItem(enclosing));
    assertThat(visitedNodes, not(hasItem(enclosed)));
  }
}
