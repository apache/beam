///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.dataartisans.flink.dataflow;
//
//import com.google.api.services.bigquery.model.TableRow;
//import com.google.cloud.dataflow.sdk.Pipeline;
//import com.google.cloud.dataflow.sdk.io.TextIO;
//import com.google.cloud.dataflow.sdk.transforms.Create;
//import com.google.cloud.dataflow.sdk.values.PCollection;
//import com.google.common.base.Joiner;
//import org.apache.flink.test.util.JavaProgramTestBase;
//
//import java.util.Arrays;
//
//public class TopWikipediaSessionsITCase extends JavaProgramTestBase {
//	protected String resultPath;
//
//	public TopWikipediaSessionsITCase(){
//	}
//
//	static final String[] EXPECTED_RESULT = new String[] {
//			"user1 : [1970-01-01T00:00:00.000Z..1970-01-01T01:00:02.000Z)"
//					+ " : 3 : 1970-01-01T00:00:00.000Z",
//			"user3 : [1970-02-05T00:00:00.000Z..1970-02-05T01:00:00.000Z)"
//					+ " : 1 : 1970-02-01T00:00:00.000Z" };
//
//	@Override
//	protected void preSubmit() throws Exception {
//		resultPath = getTempDirPath("result");
//	}
//
//	@Override
//	protected void postSubmit() throws Exception {
//		compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
//	}
//
//	@Override
//	protected void testProgram() throws Exception {
//
//		Pipeline p = FlinkTestPipeline.create();
//
//		PCollection<String> output =
//				p.apply(Create.of(Arrays.asList(new TableRow().set("timestamp", 0).set
//						("contributor_username", "user1"), new TableRow().set("timestamp", 1).set
//						("contributor_username", "user1"), new TableRow().set("timestamp", 2).set
//						("contributor_username", "user1"), new TableRow().set("timestamp", 0).set
//						("contributor_username", "user2"), new TableRow().set("timestamp", 1).set
//						("contributor_username", "user2"), new TableRow().set("timestamp", 3601).set
//						("contributor_username", "user2"), new TableRow().set("timestamp", 3602).set
//						("contributor_username", "user2"), new TableRow().set("timestamp", 35 * 24 * 3600)
//						.set("contributor_username", "user3"))))
//						.apply(new TopWikipediaSessions.ComputeTopSessions(1.0));
//
//		output.apply(TextIO.Write.to(resultPath));
//
//		p.run();
//	}
//}
