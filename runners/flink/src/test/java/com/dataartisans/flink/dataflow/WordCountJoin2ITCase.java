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
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Joiner;
import org.apache.flink.test.util.JavaProgramTestBase;


public class WordCountJoin2ITCase extends JavaProgramTestBase {

	static final String[] WORDS_1 = new String[] {
			"hi there", "hi", "hi sue bob",
			"hi sue", "", "bob hi"};

	static final String[] WORDS_2 = new String[] {
			"hi tim", "beauty", "hooray sue bob",
			"hi there", "", "please say hi"};

	static final String[] RESULTS = new String[] {
			"beauty -> Tag1: Tag2: 1",
			"bob -> Tag1: 2 Tag2: 1",
			"hi -> Tag1: 5 Tag2: 3",
			"hooray -> Tag1: Tag2: 1",
			"please -> Tag1: Tag2: 1",
			"say -> Tag1: Tag2: 1",
			"sue -> Tag1: 2 Tag2: 1",
			"there -> Tag1: 1 Tag2: 1",
			"tim -> Tag1: Tag2: 1"
	};

	static final TupleTag<Long> tag1 = new TupleTag<>("Tag1");
	static final TupleTag<Long> tag2 = new TupleTag<>("Tag2");

	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(Joiner.on('\n').join(RESULTS), resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		Pipeline p = FlinkTestPipeline.create();

		/* Create two PCollections and join them */
		PCollection<KV<String,Long>> occurences1 = p.apply(Create.of(WORDS_1))
				.apply(ParDo.of(new ExtractWordsFn()))
				.apply(Count.<String>perElement());

		PCollection<KV<String,Long>> occurences2 = p.apply(Create.of(WORDS_2))
				.apply(ParDo.of(new ExtractWordsFn()))
				.apply(Count.<String>perElement());

		/* CoGroup the two collections */
		PCollection<KV<String, CoGbkResult>> mergedOccurences = KeyedPCollectionTuple
				.of(tag1, occurences1)
				.and(tag2, occurences2)
				.apply(CoGroupByKey.<String>create());

		/* Format output */
		mergedOccurences.apply(ParDo.of(new FormatCountsFn()))
				.apply(TextIO.Write.named("test").to(resultPath));

		p.run();
	}


	static class ExtractWordsFn extends DoFn<String, String> {

		@Override
		public void startBundle(Context c) {
		}

		@Override
		public void processElement(ProcessContext c) {
			// Split the line into words.
			String[] words = c.element().split("[^a-zA-Z']+");

			// Output each word encountered into the output PCollection.
			for (String word : words) {
				if (!word.isEmpty()) {
					c.output(word);
				}
			}
		}
	}

	static class FormatCountsFn extends DoFn<KV<String, CoGbkResult>, String> {
		@Override
		public void processElement(ProcessContext c) {
			CoGbkResult value = c.element().getValue();
			String key = c.element().getKey();
			String countTag1 = tag1.getId() + ": ";
			String countTag2 = tag2.getId() + ": ";
			for (Long count : value.getAll(tag1)) {
				countTag1 += count + " ";
			}
			for (Long count : value.getAll(tag2)) {
				countTag2 += count;
			}
			c.output(key + " -> " + countTag1 + countTag2);
		}
	}


}
