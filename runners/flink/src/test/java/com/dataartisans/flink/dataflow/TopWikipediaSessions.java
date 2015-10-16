///*
// * Copyright (C) 2015 Google Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//
//package com.dataartisans.flink.dataflow;
//
//import com.google.api.services.bigquery.model.TableRow;
//import com.google.cloud.dataflow.sdk.Pipeline;
//import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
//import com.google.cloud.dataflow.sdk.io.TextIO;
//import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
//import com.google.cloud.dataflow.sdk.options.Default;
//import com.google.cloud.dataflow.sdk.options.Description;
//import com.google.cloud.dataflow.sdk.options.PipelineOptions;
//import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
//import com.google.cloud.dataflow.sdk.options.Validation;
//import com.google.cloud.dataflow.sdk.transforms.Count;
//import com.google.cloud.dataflow.sdk.transforms.DoFn;
//import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
//import com.google.cloud.dataflow.sdk.transforms.PTransform;
//import com.google.cloud.dataflow.sdk.transforms.ParDo;
//import com.google.cloud.dataflow.sdk.transforms.SerializableComparator;
//import com.google.cloud.dataflow.sdk.transforms.Top;
//import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows;
//import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
//import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
//import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
//import com.google.cloud.dataflow.sdk.values.KV;
//import com.google.cloud.dataflow.sdk.values.PCollection;
//
//import org.joda.time.Duration;
//import org.joda.time.Instant;
//
//import java.util.List;
//
///**
// * Copied from {@link com.google.cloud.dataflow.examples.complete.TopWikipediaSessions} because the code
// * is private there.
// */
//public class TopWikipediaSessions {
//	private static final String EXPORTED_WIKI_TABLE = "gs://dataflow-samples/wikipedia_edits/*.json";
//
//	/**
//	 * Extracts user and timestamp from a TableRow representing a Wikipedia edit.
//	 */
//	static class ExtractUserAndTimestamp extends DoFn<TableRow, String> {
//		private static final long serialVersionUID = 0;
//
//		@Override
//		public void processElement(ProcessContext c) {
//			TableRow row = c.element();
//			int timestamp = (Integer) row.get("timestamp");
//			String userName = (String) row.get("contributor_username");
//			if (userName != null) {
//				// Sets the implicit timestamp field to be used in windowing.
//				c.outputWithTimestamp(userName, new Instant(timestamp * 1000L));
//			}
//		}
//	}
//
//	/**
//	 * Computes the number of edits in each user session.  A session is defined as
//	 * a string of edits where each is separated from the next by less than an hour.
//	 */
//	static class ComputeSessions
//			extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
//		private static final long serialVersionUID = 0;
//
//		@Override
//		public PCollection<KV<String, Long>> apply(PCollection<String> actions) {
//			return actions
//					.apply(Window.<String>into(Sessions.withGapDuration(Duration.standardHours(1))))
//
//					.apply(Count.<String>perElement());
//		}
//	}
//
//	/**
//	 * Computes the longest session ending in each month.
//	 */
//	private static class TopPerMonth
//			extends PTransform<PCollection<KV<String, Long>>, PCollection<List<KV<String, Long>>>> {
//		private static final long serialVersionUID = 0;
//
//		@Override
//		public PCollection<List<KV<String, Long>>> apply(PCollection<KV<String, Long>> sessions) {
//			return sessions
//					.apply(Window.<KV<String, Long>>into(CalendarWindows.months(1)))
//
//					.apply(Top.of(1, new SerializableComparator<KV<String, Long>>() {
//						private static final long serialVersionUID = 0;
//
//						@Override
//						public int compare(KV<String, Long> o1, KV<String, Long> o2) {
//							return Long.compare(o1.getValue(), o2.getValue());
//						}
//					}).withoutDefaults());
//		}
//	}
//
//	static class SessionsToStringsDoFn extends DoFn<KV<String, Long>, KV<String, Long>>
//			implements RequiresWindowAccess {
//
//		private static final long serialVersionUID = 0;
//
//		@Override
//		public void processElement(ProcessContext c) {
//			c.output(KV.of(
//					c.element().getKey() + " : " + c.window(), c.element().getValue()));
//		}
//	}
//
//	static class FormatOutputDoFn extends DoFn<List<KV<String, Long>>, String>
//			implements RequiresWindowAccess {
//		private static final long serialVersionUID = 0;
//
//		@Override
//		public void processElement(ProcessContext c) {
//			for (KV<String, Long> item : c.element()) {
//				String session = item.getKey();
//				long count = item.getValue();
//				c.output(session + " : " + count + " : " + ((IntervalWindow) c.window()).start());
//			}
//		}
//	}
//
//	static class ComputeTopSessions extends PTransform<PCollection<TableRow>, PCollection<String>> {
//
//		private static final long serialVersionUID = 0;
//
//		private final double samplingThreshold;
//
//		public ComputeTopSessions(double samplingThreshold) {
//			this.samplingThreshold = samplingThreshold;
//		}
//
//		@Override
//		public PCollection<String> apply(PCollection<TableRow> input) {
//			return input
//					.apply(ParDo.of(new ExtractUserAndTimestamp()))
//
//					.apply(ParDo.named("SampleUsers").of(
//							new DoFn<String, String>() {
//								private static final long serialVersionUID = 0;
//
//								@Override
//								public void processElement(ProcessContext c) {
//									if (Math.abs(c.element().hashCode()) <= Integer.MAX_VALUE * samplingThreshold) {
//										c.output(c.element());
//									}
//								}
//							}))
//
//					.apply(new ComputeSessions())
//
//					.apply(ParDo.named("SessionsToStrings").of(new SessionsToStringsDoFn()))
//					.apply(new TopPerMonth())
//					.apply(ParDo.named("FormatOutput").of(new FormatOutputDoFn()));
//		}
//	}
//
//	/**
//	 * Options supported by this class.
//	 *
//	 * <p> Inherits standard Dataflow configuration options.
//	 */
//	private static interface Options extends PipelineOptions {
//		@Description(
//				"Input specified as a GCS path containing a BigQuery table exported as json")
//		@Default.String(EXPORTED_WIKI_TABLE)
//		String getInput();
//		void setInput(String value);
//
//		@Description("File to output results to")
//		@Validation.Required
//		String getOutput();
//		void setOutput(String value);
//	}
//
//	public static void main(String[] args) {
//		Options options = PipelineOptionsFactory.fromArgs(args)
//				.withValidation()
//				.as(Options.class);
//		DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
//
//		Pipeline p = Pipeline.create(dataflowOptions);
//
//		double samplingThreshold = 0.1;
//
//		p.apply(TextIO.Read
//				.from(options.getInput())
//				.withCoder(TableRowJsonCoder.of()))
//				.apply(new ComputeTopSessions(samplingThreshold))
//				.apply(TextIO.Write.named("Write").withoutSharding().to(options.getOutput()));
//
//		p.run();
//	}
//}
