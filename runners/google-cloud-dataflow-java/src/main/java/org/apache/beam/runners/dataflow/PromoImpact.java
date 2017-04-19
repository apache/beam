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
package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.CalendarWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * An example that computes the most impactful employees in a given promo period.
 *
 * Note: This is really just an example of using potential new {@link Aggregator} functionality,
 * and obviously is not intended to be a real data pipeline.
 */
public class PromoImpact {
  interface Options extends PipelineOptions {
    String getCodeCheckinsTopic();
    void setCodeCheckinsTopic(String value);

    String getStockSymbol();
    void setStockSymbol(String value);

    String getResultsTable();
    void setResultsTable(String value);
  }

  @AutoValue
  abstract static class CodeCheckinEvent {
    public static CodeCheckinEvent of(String user, String checkinHash) {
      return new AutoValue_PromoImpact_CodeCheckinEvent(user, checkinHash);
    }

    abstract String getUser();
    abstract String getCheckinHash();
  }

  @AutoValue
  abstract static class CheckinWithStockPrice {
    public static CheckinWithStockPrice of(String user, double price) {
      return new AutoValue_PromoImpact_CheckinWithStockPrice(user, price);
    }

    abstract String getUser();
    abstract double getStockPrice();
  }

  static class CorrelateEventWithStockPrice extends DoFn<CodeCheckinEvent, CheckinWithStockPrice> {
    private final String stockSymbol;
    private final Aggregator<TimestampedValue<Double>, Double> priceAggregator;

    public CorrelateEventWithStockPrice(String stockSymbol) {
      this.stockSymbol = stockSymbol;
      this.priceAggregator = createAggregator("stockPrice", new Latest.LatestDoubleFn());
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String user = c.element().getUser();
      double stockPrice = getLatestStockPrice(stockSymbol);
      c.output(CheckinWithStockPrice.of(user, stockPrice));
    }

    private Instant start = Instant.now();
    private double getLatestStockPrice(String symbol) {
      // Super-accurate calculation
      Instant now = Instant.now();
      double price = (symbol.hashCode() % 1000) + (3 * new Duration(start, now).getStandardMinutes());

      priceAggregator.addValue(TimestampedValue.of(price, now));
      return price;
    }

    public Aggregator<TimestampedValue<Double>, Double> getPriceAggregator() {
      return priceAggregator;
    }
  }

  private static class CalculateUserImpact extends Combine.CombineFn<CheckinWithStockPrice, Double, Double> {
    @Override public Double createAccumulator() { return 0.0; }

    @Override
    public Double addInput(Double accumulator, CheckinWithStockPrice input) {
      return accumulator + input.getStockPrice();
    }

    @Override
    public Double mergeAccumulators(Iterable<Double> accumulators) {
      Double result = createAccumulator();
      for (Double accumulator : accumulators) {
        result += accumulator;
      }
      return result;
    }

    @Override public Double extractOutput(Double accumulator) { return accumulator; }
  }

  public static void main(String[] args) throws IOException, InterruptedException, AggregatorRetrievalException {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
    options.setRunner(BlockingDataflowRunner.class);

    Pipeline p = Pipeline.create(options);

    PubsubIO.Read.Bound<CodeCheckinEvent> pubsubRead = PubsubIO.Read
        .topic(options.getCodeCheckinsTopic())
        .withCoder(AvroCoder.of(CodeCheckinEvent.class));
    PCollection<CodeCheckinEvent> events = p.apply("Get Code Checkins", pubsubRead);

    CorrelateEventWithStockPrice stockPriceFn = new CorrelateEventWithStockPrice(options.getStockSymbol());
    PCollection<CheckinWithStockPrice> withStockPrice = events.apply("Lookup Stock Price", ParDo.of(stockPriceFn));

    PCollection<KV<String, Double>> impact = withStockPrice
        .apply("Window by Quarter", Window.<CheckinWithStockPrice>into(CalendarWindows.months(3)))
        .apply("Key by User", WithKeys.of(new SerializableFunction<CheckinWithStockPrice, String> () {
          @Override public String apply(CheckinWithStockPrice input) { return input.getUser(); }
        }))
        .apply("Calculate User Impact", Combine.<String, CheckinWithStockPrice, Double>perKey(new CalculateUserImpact()));

    BigQueryIO.Write.Bound bigQueryWrite = BigQueryIO.Write.to(options.getResultsTable());
    impact
        .apply("Format Results", ParDo.of(new DoFn<KV<String, Double>, TableRow>() {}))
        .apply("Output Results", bigQueryWrite);

    PipelineResult result = p.run();
    p.wait();

    System.out.println(String.format("Pubsub subscription: %s",
        result.getAggregatorValues(pubsubRead.getSubscriptionAggregator())));

    System.out.println(String.format("Stock Price: %s",
        result.getAggregatorValues(stockPriceFn.getPriceAggregator())));

    System.out.println(String.format("Big Query Export: %s",
        result.getAggregatorValues(bigQueryWrite.getExportJobAggregator())));
  }
}
