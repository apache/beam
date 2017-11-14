/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.benchmarks.beam;

import cz.seznam.euphoria.benchmarks.model.Benchmarks;
import cz.seznam.euphoria.benchmarks.model.windowing.Time;
import cz.seznam.euphoria.benchmarks.model.windowing.TimeSliding;
import cz.seznam.euphoria.benchmarks.model.windowing.Windowing;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.runners.flink.translation.types.FlinkCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.time.Duration;
import java.util.Comparator;

@SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
public class BeamTrends {

  public static void main(PipelineOptions opts, Parameters params, String exec) {
    int smooth = params.getRankSmoothness();
    Duration longInterval = params.getLongStats();
    Duration shortInterval = params.getShortStats();

    // define windowings
    Windowing longWindowing = TimeSliding.of(longInterval, shortInterval);
    Windowing shortWindowing = Time.of(shortInterval);

    // get input data
    Pipeline ppl = Pipeline.create(opts);
    PCollection<Tuple2<Long, String>> input = Util.createInput(ppl, params);

    // expand windows
    PCollection<Tuple2<Long, String>> longWindows = expandWindows("LongWindowing", input, longWindowing);
    PCollection<KV<Tuple2<Long, String>, Long>> longStats = longWindows.apply("LongCount", Count.perElement());

    PCollection<Tuple2<Long, String>> shortWindows = expandWindows("ShortWindowing", input, shortWindowing);
    PCollection<KV<Tuple2<Long, String>, Long>> shortStats = shortWindows.apply("ShortCount", Count.perElement());

    // join on window + query
    final TupleTag<Long> longStatsTag = new TupleTag<>();
    final TupleTag<Long> shortStatsTag = new TupleTag<>();
    PCollection<KV<Tuple2<Long, String>, CoGbkResult>> cogrouped = KeyedPCollectionTuple
        .of(longStatsTag, longStats)
        .and(shortStatsTag, shortStats)
        .apply("CoGroupByQuery", CoGroupByKey.<Tuple2<Long, String>>create());

    // map joined to window only
    PCollection<KV<Long, Tuple2<String, Double>>> ranked = cogrouped
        .apply("MapJoined", ParDo.of(new DoFn<KV<Tuple2<Long, String>, CoGbkResult>, KV<Long, Tuple2<String, Double>>>() {
          @ProcessElement
          public void processElements(ProcessContext c) {
            Tuple2<Long, String> key = c.element().getKey();
            CoGbkResult value = c.element().getValue();
            int longCount = value.getOnly(longStatsTag, 0L).intValue();
            int shortCount = value.getOnly(shortStatsTag, 0L).intValue();
            if (longCount > 0 && shortCount > 0) {
              double rank = Benchmarks.trendsRank(
                  longInterval.toMillis(), longCount,
                  shortInterval.toMillis(), shortCount,
                  smooth);
              c.output(KV.of(key.f0, Tuple2.of(key.f1, rank)));
            }
          }
        })).setCoder(KvCoder.of(VarLongCoder.of(), new FlinkCoder<>(new TypeHint<Tuple2<String, Double>>() {
        }.getTypeInfo(), new ExecutionConfig())));

    // find max in windows
    PCollection<KV<Long, Tuple2<String, Double>>> max = ranked.apply(Max.perKey(new RankComparator()));

    Util.output(max, params, BeamTrends.class.getSimpleName() + "_" + exec);

    try {
      ppl.run().waitUntilFinish();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static class RankComparator implements Comparator<Tuple2<String, Double>>, Serializable {
    @Override
    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
      return o1.f1.compareTo(o2.f1);
    }
  }

  private static PCollection<Tuple2<Long, String>> expandWindows(String name, PCollection<Tuple2<Long, String>> input,
      Windowing windowing) {
    return input.apply(name, ParDo.of(new DoFn<Tuple2<Long, String>, Tuple2<Long, String>>() {
      @ProcessElement
      public void processElements(ProcessContext c) {
        Tuple2<Long, String> el = c.element();
        // assign windows
        Iterable<Long> windows = windowing.generate(el.f0);
        for (Long w : windows) {
          c.output(Tuple2.of(w, el.f1));
        }
      }
    })).setCoder(new FlinkCoder<>(new TypeHint<Tuple2<Long, String>>() {
    }.getTypeInfo(), new ExecutionConfig()));
  }
}
