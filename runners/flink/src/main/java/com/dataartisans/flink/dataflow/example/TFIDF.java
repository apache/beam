package com.dataartisans.flink.dataflow.example;

import com.google.cloud.dataflow.examples.TfIdf;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringDelegateCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.common.collect.Iterables;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TFIDF {

	private static interface Options extends PipelineOptions {
		@Description("Path to the directory or GCS prefix containing files to read from")
		@Default.String("gs://dataflow-samples/shakespeare/")
		String getInput();
		void setInput(String value);

		@Description("Prefix of output URI to write to")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}

	public static Set<URI> listInputDocuments(Options options)
			throws URISyntaxException, IOException {
		URI baseUri = new URI(options.getInput());

		// List all documents in the directory or GCS prefix.
		URI absoluteUri;
		if (baseUri.getScheme() != null) {
			absoluteUri = baseUri;
		} else {
			absoluteUri = new URI(
					"file",
					baseUri.getAuthority(),
					baseUri.getPath(),
					baseUri.getQuery(),
					baseUri.getFragment());
		}

		Set<URI> uris = new HashSet<>();
		if (absoluteUri.getScheme().equals("file")) {
			File directory = new File(absoluteUri);
			for (String entry : directory.list()) {
				File path = new File(directory, entry);
				uris.add(path.toURI());
			}
		} else if (absoluteUri.getScheme().equals("gs")) {
			GcsUtil gcsUtil = options.as(GcsOptions.class).getGcsUtil();
			URI gcsUriGlob = new URI(
					absoluteUri.getScheme(),
					absoluteUri.getAuthority(),
					absoluteUri.getPath() + "*",
					absoluteUri.getQuery(),
					absoluteUri.getFragment());
			for (GcsPath entry : gcsUtil.expand(GcsPath.fromUri(gcsUriGlob))) {
				uris.add(entry.toUri());
			}
		}

		return uris;
	}

	static class SimpleCombineFn<V> extends Combine.CombineFn<V, List<V>, V> {
		/**
		 * Returns a {@code CombineFn} that uses the given
		 * {@code SerializableFunction} to combine values.
		 */
		public static <V> SimpleCombineFn<V> of(
				SerializableFunction<Iterable<V>, V> combiner) {
			return new SimpleCombineFn<>(combiner);
		}

		/**
		 * The number of values to accumulate before invoking the combiner
		 * function to combine them.
		 */
		private static final int BUFFER_SIZE = 20;

		/** The combiner function. */
		private final SerializableFunction<Iterable<V>, V> combiner;

		private SimpleCombineFn(SerializableFunction<Iterable<V>, V> combiner) {
			this.combiner = combiner;
		}

		@Override
		public List<V> createAccumulator() {
			return new ArrayList<>();
		}

		@Override
		public void addInput(List<V> accumulator, V input) {
			accumulator.add(input);
			if (accumulator.size() > BUFFER_SIZE) {
				V combined = combiner.apply(accumulator);
				accumulator.clear();
				accumulator.add(combined);
			}
		}

		@Override
		public List<V> mergeAccumulators(Iterable<List<V>> accumulators) {
			List<V> singleton = new ArrayList<>();
			singleton.add(combiner.apply(Iterables.concat(accumulators)));
			return singleton;
		}

		@Override
		public V extractOutput(List<V> accumulator) {
			return combiner.apply(accumulator);
		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		Options options = PipelineOptionsFactory.create().as(Options.class);
		options.setOutput("/tmp/output2.txt");
		options.setInput("/tmp/documents");
		options.setRunner(DirectPipelineRunner.class);
		//options.setRunner(FlinkLocalPipelineRunner.class);

		Pipeline p = Pipeline.create(options);

		p.getCoderRegistry().registerCoder(URI.class, StringDelegateCoder.of(URI.class));

		List<URI> documentURLs = new ArrayList<URI>();


		p.apply(new TfIdf.ReadDocuments(listInputDocuments(options)))
						.apply(new TfIdf.ComputeTfIdf())
						.apply(ParDo.of(new DoFn<Object, String>() {
							@Override
							public void processElement(ProcessContext c) throws Exception {
								c.output(c.element().toString());
							}
						}))
						.apply(TextIO.Write.to("/tmp/output"));
		//.apply(new TfIdf.WriteTfIdf(options.getOutput()));

		p.run();
	}
}