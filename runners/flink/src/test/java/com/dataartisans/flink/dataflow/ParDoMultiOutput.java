///*
// * Copyright 2015 Data Artisans GmbH
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.dataartisans.flink.dataflow.examples;
//
//import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
//import com.dataartisans.flink.dataflow.io.ConsoleIO;
//import com.google.cloud.dataflow.examples.WordCount.Options;
//import com.google.cloud.dataflow.sdk.Pipeline;
//import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
//import com.google.cloud.dataflow.sdk.transforms.Create;
//import com.google.cloud.dataflow.sdk.transforms.DoFn;
//import com.google.cloud.dataflow.sdk.transforms.ParDo;
//import com.google.cloud.dataflow.sdk.values.PCollection;
//import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
//import com.google.cloud.dataflow.sdk.values.TupleTag;
//import com.google.cloud.dataflow.sdk.values.TupleTagList;
//
//public class ParDoMultiOutput {
//
//	public static void main(String[] args) {
//
//		Options options = PipelineOptionsFactory.create().as(Options.class);
//		options.setOutput("/tmp/output2.txt");
//		options.setInput("/tmp/documents/hello_world.txt");
//		//options.setRunner(DirectPipelineRunner.class);
//		options.setRunner(FlinkPipelineRunner.class);
//
//		Pipeline p = Pipeline.create(options);
//
//		PCollection<String> words = p.apply(Create.of("Hello", "Whatupmyman", "hey", "SPECIALthere", "MAAA", "MAAFOOO"));
//
//		// Select words whose length is below a cut off,
//		// plus the lengths of words that are above the cut off.
//		// Also select words starting with "MARKER".
//		final int wordLengthCutOff = 3;
//		// Create tags to use for the main and side outputs.
//		final TupleTag<String> wordsBelowCutOffTag = new TupleTag<String>(){};
//		final TupleTag<Integer> wordLengthsAboveCutOffTag = new TupleTag<Integer>(){};
//		final TupleTag<String> markedWordsTag = new TupleTag<String>(){};
//
//		PCollectionTuple results =
//		 words.apply(ParDo
//				 .withOutputTags(wordsBelowCutOffTag, TupleTagList.of(wordLengthsAboveCutOffTag)
//					     .and(markedWordsTag))
//			     .of(new DoFn<String, String>() {
//				     final TupleTag<String> specialWordsTag = new TupleTag<String>() {
//				     };
//
//				     public void processElement(ProcessContext c) {
//					     String word = c.element();
//					     if (word.length() <= wordLengthCutOff) {
//						     c.output(word);
//					     } else {
//						     c.sideOutput(wordLengthsAboveCutOffTag, word.length());
//					     }
//					     if (word.startsWith("MAA")) {
//						     c.sideOutput(markedWordsTag, word);
//					     }
//
//					     if (word.startsWith("SPECIAL")) {
//						     c.sideOutput(specialWordsTag, word);
//					     }
//				     }
//			     }));
//
//		// Extract the PCollection results, by tag.
//		PCollection<String> wordsBelowCutOff = results.get(wordsBelowCutOffTag);
//		PCollection<Integer> wordLengthsAboveCutOff = results.get
//		     (wordLengthsAboveCutOffTag);
//	    PCollection<String> markedWords = results.get(markedWordsTag);
//
//		markedWords.apply(ConsoleIO.Write.create());
//
//		p.run();
//	}
//}
