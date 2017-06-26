

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
package com.alibaba.jstorm.beam.example;

import avro.shaded.com.google.common.collect.Maps;
import com.alibaba.jstorm.beam.StormPipelineOptions;
import com.alibaba.jstorm.beam.StormRunner;
import com.alibaba.jstorm.utils.LoadConf;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A minimal word count pipeline using the Beam API, running on top of Storm
 * 
 * When the Storm Runner is reasonably complete, running this pipline in Storm should yield that same output as running it on the Beam DirectRunner
 * 
 */
public class StormWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(StormWordCount.class);

    static class ExtractWordsFn extends DoFn<String, String> {
        private static final long serialVersionUID = -664504699658016696L;
        private final Counter emptyLines = Metrics.counter("global", "emptyLines");

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            //LOG.info("Receive Element={}, timeStamp={}, assignWindows={}",
            //        c.element(), c.timestamp().toDateTime(), window);
            if (c.element().trim().isEmpty()) {
                emptyLines.inc();
            }

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

    /**
     * A SimpleFunction that converts a Word and Count into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        private static final long serialVersionUID = 7683316640746784718L;

        @Override
        public String apply(KV<String, Long> input) {
            String retval = input.getKey() + ": " + input.getValue();
            LOG.info("count output: " + retval);
            return retval;
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of formatted word counts.
     */
    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        private static final long serialVersionUID = -8699941132143619425L;

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.<String> perElement());

            return wordCounts;
        }
    }

    /**
     * Options supported by {@link StormWordCount}.
     * <p>
     * <p>
     * Inherits standard configuration options.
     */
    public interface WordCountOptions extends StormPipelineOptions {

    }

    public static Map loadConf(String arg) {
        if (arg.endsWith("yaml")) {
            return LoadConf.LoadYaml(arg);
        } else {
            return LoadConf.LoadProperty(arg);
        }
    }

    public static void main(String[] args) {
        Map conf = Maps.newHashMap();
        if (args.length > 0) {
            conf.putAll(loadConf(args[0]));
        }

        WordCountOptions options = PipelineOptionsFactory.as(WordCountOptions.class);
        options.setRunner(StormRunner.class);
        options.setJobName("WordCount");

        int pn = conf.containsKey("parallelism.number") ? (Integer) conf.get("parallelism.number") : 1;
        options.setParallelismNumber(pn);

        if (conf.containsKey("spout.parallelism.num")) {
            options.getParallelismNumMap().put("Spout", (Integer) conf.get("spout.parallelism.num"));
        }
        if (conf.containsKey(("window.parallelism.num"))) {
            options.getParallelismNumMap().put("Window", (Integer) conf.get("window.parallelism.num"));
        }
        if(conf.containsKey("count.parallelism.num")) {
            options.getParallelismNumMap().put("CountWords", (Integer) conf.get("count.parallelism.num"));
        }
        if(conf.containsKey("format.parallelism.num")) {
            options.getParallelismNumMap().put("FormatAsText", (Integer) conf.get("format.parallelism.num"));
        }

        if (conf.size() > 0) {
            options.setTopologyConfig(conf);
        }

        Pipeline p = Pipeline.create(options);
        p.apply("Spout", Read.from(new RandomSentenceSource(StringUtf8Coder.of())))
                .apply("Window", Window.<String> into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply("CountWords", new CountWords())
                .apply("FormatAsText", MapElements.via(new FormatAsTextFn()));

        p.run();

    }
}
