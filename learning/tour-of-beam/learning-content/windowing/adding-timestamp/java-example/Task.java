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

// beam-playground:
//   name: adding-timestamp
//   description: Adding timestamp example.
//   multifile: false
//   context_line: 47
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import java.io.Serializable;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Event> input =
                pipeline.apply(
                        Create.of(
                                new Event("1", "book-order", DateTime.parse("2019-06-01T00:00:00+00:00")),
                                new Event("2", "pencil-order", DateTime.parse("2019-06-02T00:00:00+00:00")),
                                new Event("3", "paper-order", DateTime.parse("2019-06-03T00:00:00+00:00")),
                                new Event("4", "pencil-order", DateTime.parse("2019-06-04T00:00:00+00:00")),
                                new Event("5", "book-order", DateTime.parse("2019-06-05T00:00:00+00:00"))
                        )
                );

        PCollection<Event> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput()));

        pipeline.run();
    }

    static PCollection<Event> applyTransform(PCollection<Event> events) {
        return events.apply(ParDo.of(new DoFn<Event, Event>() {

            @ProcessElement
            public void processElement(@Element Event event, OutputReceiver<Event> out) {
                out.outputWithTimestamp(event, event.getDate().toInstant());
            }

        }));
    }

    static class LogOutput<T> extends DoFn<T, T> {

        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }

}

public class Event implements Serializable {

    private String id;
    private String event;
    private DateTime date;

    public Event(String id, String event, DateTime date) {
        this.id = id;
        this.event = event;
        this.date = date;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public DateTime getDate() {
        return date;
    }

    public void setDate(DateTime date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Event event1 = (Event) o;

        return id.equals(event1.id) &&
                event.equals(event1.event) &&
                date.equals(event1.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, event, date);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", event='" + event + '\'' +
                ", date=" + date +
                '}';
    }

}