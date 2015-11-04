/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.common.base.Joiner;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.Serializable;

public class ParDoMultiOutputITCase extends JavaProgramTestBase implements Serializable {

	private String resultPath;

	private static String[] expectedWords = {"MAAA", "MAAFOOO"};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(Joiner.on("\n").join(expectedWords), resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		Pipeline p = FlinkTestPipeline.create();

		PCollection<String> words = p.apply(Create.of("Hello", "Whatupmyman", "hey", "SPECIALthere", "MAAA", "MAAFOOO"));

		// Select words whose length is below a cut off,
		// plus the lengths of words that are above the cut off.
		// Also select words starting with "MARKER".
		final int wordLengthCutOff = 3;
		// Create tags to use for the main and side outputs.
		final TupleTag<String> wordsBelowCutOffTag = new TupleTag<String>(){};
		final TupleTag<Integer> wordLengthsAboveCutOffTag = new TupleTag<Integer>(){};
		final TupleTag<String> markedWordsTag = new TupleTag<String>(){};

		PCollectionTuple results =
				words.apply(ParDo
						.withOutputTags(wordsBelowCutOffTag, TupleTagList.of(wordLengthsAboveCutOffTag)
								.and(markedWordsTag))
						.of(new DoFn<String, String>() {
							final TupleTag<String> specialWordsTag = new TupleTag<String>() {
							};

							public void processElement(ProcessContext c) {
								String word = c.element();
								if (word.length() <= wordLengthCutOff) {
									c.output(word);
								} else {
									c.sideOutput(wordLengthsAboveCutOffTag, word.length());
								}
								if (word.startsWith("MAA")) {
									c.sideOutput(markedWordsTag, word);
								}

								if (word.startsWith("SPECIAL")) {
									c.sideOutput(specialWordsTag, word);
								}
							}
						}));

		// Extract the PCollection results, by tag.
		PCollection<String> wordsBelowCutOff = results.get(wordsBelowCutOffTag);
		PCollection<Integer> wordLengthsAboveCutOff = results.get
				(wordLengthsAboveCutOffTag);
		PCollection<String> markedWords = results.get(markedWordsTag);

		markedWords.apply(TextIO.Write.to(resultPath));

		p.run();
	}
}
