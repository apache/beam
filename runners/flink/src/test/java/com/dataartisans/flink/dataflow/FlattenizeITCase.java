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
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.common.base.Joiner;
import org.apache.flink.test.util.JavaProgramTestBase;

public class FlattenizeITCase extends JavaProgramTestBase {

	private String resultPath;
	private String resultPath2;

	private static final String[] words = {"hello", "this", "is", "a", "DataSet!"};
	private static final String[] words2 = {"hello", "this", "is", "another", "DataSet!"};
	private static final String[] words3 = {"hello", "this", "is", "yet", "another", "DataSet!"};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
		resultPath2 = getTempDirPath("result2");
	}

	@Override
	protected void postSubmit() throws Exception {
		String join = Joiner.on('\n').join(words);
		String join2 = Joiner.on('\n').join(words2);
		String join3 = Joiner.on('\n').join(words3);
		compareResultsByLinesInMemory(join + "\n" + join2, resultPath);
		compareResultsByLinesInMemory(join + "\n" + join2 + "\n" + join3, resultPath2);
	}


	@Override
	protected void testProgram() throws Exception {
		Pipeline p = FlinkTestPipeline.create();

		PCollection<String> p1 = p.apply(Create.of(words));
		PCollection<String> p2 = p.apply(Create.of(words2));

		PCollectionList<String> list = PCollectionList.of(p1).and(p2);

		list.apply(Flatten.<String>pCollections()).apply(TextIO.Write.to(resultPath));

		PCollection<String> p3 = p.apply(Create.of(words3));

		PCollectionList<String> list2 = list.and(p3);

		list2.apply(Flatten.<String>pCollections()).apply(TextIO.Write.to(resultPath2));

		p.run();
	}

}
