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
//import com.google.cloud.dataflow.examples.TfIdf;
//import com.google.cloud.dataflow.sdk.Pipeline;
//import com.google.cloud.dataflow.sdk.coders.StringDelegateCoder;
//import com.google.cloud.dataflow.sdk.io.TextIO;
//import com.google.cloud.dataflow.sdk.options.*;
//import com.google.cloud.dataflow.sdk.transforms.DoFn;
//import com.google.cloud.dataflow.sdk.transforms.ParDo;
//import com.google.cloud.dataflow.sdk.util.GcsUtil;
//import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
//import com.google.cloud.dataflow.sdk.values.KV;
//
//import java.io.File;
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//public class FlattenizeThis {
//
//	private static interface Options extends PipelineOptions {
//		@Description("Path to the directory or GCS prefix containing files to read from")
//		@Default.String("gs://dataflow-samples/shakespeare/")
//		String getInput();
//		void setInput(String value);
//
//		@Description("Prefix of output URI to write to")
//		@Validation.Required
//		String getOutput();
//		void setOutput(String value);
//	}
//
//	public static Set<URI> listInputDocuments(Options options)
//			throws URISyntaxException, IOException {
//		URI baseUri = new URI(options.getInput());
//
//		// List all documents in the directory or GCS prefix.
//		URI absoluteUri;
//		if (baseUri.getScheme() != null) {
//			absoluteUri = baseUri;
//		} else {
//			absoluteUri = new URI(
//					"file",
//					baseUri.getAuthority(),
//					baseUri.getPath(),
//					baseUri.getQuery(),
//					baseUri.getFragment());
//		}
//
//		Set<URI> uris = new HashSet<>();
//		if (absoluteUri.getScheme().equals("file")) {
//			File directory = new File(absoluteUri);
//			for (String entry : directory.list()) {
//				File path = new File(directory, entry);
//				uris.add(path.toURI());
//			}
//		} else if (absoluteUri.getScheme().equals("gs")) {
//			GcsUtil gcsUtil = options.as(GcsOptions.class).getGcsUtil();
//			URI gcsUriGlob = new URI(
//					absoluteUri.getScheme(),
//					absoluteUri.getAuthority(),
//					absoluteUri.getPath() + "*",
//					absoluteUri.getQuery(),
//					absoluteUri.getFragment());
//			for (GcsPath entry : gcsUtil.expand(GcsPath.fromUri(gcsUriGlob))) {
//				uris.add(entry.toUri());
//			}
//		}
//
//		return uris;
//	}
//
//	public static void main(String[] args) throws IOException, URISyntaxException {
//		Options options = PipelineOptionsFactory.create().as(Options.class);
//		options.setOutput("/tmp/output2.txt");
//		options.setInput("/tmp/documents");
//		//options.setRunner(DirectPipelineRunner.class);
//		options.setRunner(FlinkPipelineRunner.class);
//
//		Pipeline p = Pipeline.create(options);
//
//		p.getCoderRegistry().registerCoder(URI.class, StringDelegateCoder.of(URI.class));
//
//
//		p.apply(new Tfidf.ReadDocuments(listInputDocuments(options)))
//				.apply(ParDo.of(new DoFn<KV<URI, String>, String>() {
//					@Override
//					public void processElement(ProcessContext c) throws Exception {
//						c.output(c.element().toString());
//					}
//				}))
//			.apply(TextIO.Write.named("WriteCounts")
//					.to(options.getOutput()));
//
//		p.run();
//	}
//}
