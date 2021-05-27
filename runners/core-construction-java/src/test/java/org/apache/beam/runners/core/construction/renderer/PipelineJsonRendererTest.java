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
package org.apache.beam.runners.core.construction.renderer;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;

/** Tests for {@link PipelineJsonRenderer}. */
@RunWith(JUnit4.class)
public class PipelineJsonRendererTest {
    @Rule public final transient TestPipeline p = TestPipeline.create();

    @Test
    public void testEmptyPipeline() {
        System.out.println(PipelineJsonRenderer.toJsonString(p));
        assertEquals("{  \"RootNode\": ["
                        + "    { \"fullName\":\"OuterMostNode\","
                        + "      \"shortName\":\"OuterMostNode\","
                        + "      \"id\":\"OuterMostNode\","
                        + "      \"ChildNode\":[    ]}],\"graphLinks\": []"
                        + "}",
                PipelineJsonRenderer.toJsonString(p).replaceAll(System.lineSeparator(), ""));
    }


    @Test
    public void testCompositePipeline() {

        p.apply(Create.timestamped(TimestampedValue.of(KV.of(1, 1), new Instant(1))))
                .apply(Window.into(FixedWindows.of(Duration.millis(10))))
                .apply(Sum.integersPerKey());

        assertEquals(("{"
                        +"  \"RootNode\": ["
                        +"    {"
                        +"      \"fullName\": \"OuterMostNode\","
                        +"      \"shortName\": \"OuterMostNode\","
                        +"      \"id\": \"OuterMostNode\","
                        +"      \"ChildNode\": ["
                        +"        {"
                        +"          \"fullName\": \"Create.TimestampedValues\","
                        +"          \"shortName\": \"Create.TimestampedValues\","
                        +"          \"id\": \"Create.TimestampedValues\","
                        +"          \"enclosingNode\": \"OuterMostNode\","
                        +"          \"ChildNode\": ["
                        +"            {"
                        +"              \"fullName\": \"Create.TimestampedValues/Create.Values\","
                        +"              \"shortName\": \"Create.Values\","
                        +"              \"id\": \"Create.TimestampedValues/Create.Values\","
                        +"              \"enclosingNode\": \"Create.TimestampedValues\","
                        +"              \"ChildNode\": ["
                        +"                {"
                        +"                  \"fullName\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)\","
                        +"                  \"shortName\": \"Read(CreateSource)\","
                        +"                  \"id\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)\","
                        +"                  \"enclosingNode\": \"Create.TimestampedValues/Create.Values\","
                        +"                  \"ChildNode\": ["
                        +"                    {"
                        +"                      \"fullName\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/Impulse\","
                        +"                      \"shortName\": \"Impulse\","
                        +"                      \"enclosingNode\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)\","
                        +"                      \"id\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/Impulse\""
                        +"                    },"
                        +"                    {"
                        +"                      \"fullName\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(OutputSingleSource)\","
                        +"                      \"shortName\": \"ParDo(OutputSingleSource)\","
                        +"                      \"id\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(OutputSingleSource)\","
                        +"                      \"enclosingNode\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)\","
                        +"                      \"ChildNode\": ["
                        +"                        {"
                        +"                          \"fullName\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource)\","
                        +"                          \"shortName\": \"ParMultiDo(OutputSingleSource)\","
                        +"                          \"enclosingNode\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(OutputSingleSource)\","
                        +"                          \"id\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource)\""
                        +"                        }"
                        +"                      ]"
                        +"                    },"
                        +"                    {"
                        +"                      \"fullName\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)\","
                        +"                      \"shortName\": \"ParDo(BoundedSourceAsSDFWrapper)\","
                        +"                      \"id\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)\","
                        +"                      \"enclosingNode\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)\","
                        +"                      \"ChildNode\": ["
                        +"                        {"
                        +"                          \"fullName\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper)\","
                        +"                          \"shortName\": \"ParMultiDo(BoundedSourceAsSDFWrapper)\","
                        +"                          \"enclosingNode\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)\","
                        +"                          \"id\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper)\""
                        +"                        }"
                        +"                      ]"
                        +"                    }"
                        +"                  ]"
                        +"                }"
                        +"              ]"
                        +"            },"
                        +"            {"
                        +"              \"fullName\": \"Create.TimestampedValues/ParDo(ConvertTimestamps)\","
                        +"              \"shortName\": \"ParDo(ConvertTimestamps)\","
                        +"              \"id\": \"Create.TimestampedValues/ParDo(ConvertTimestamps)\","
                        +"              \"enclosingNode\": \"Create.TimestampedValues\","
                        +"              \"ChildNode\": ["
                        +"                {"
                        +"                  \"fullName\": \"Create.TimestampedValues/ParDo(ConvertTimestamps)/ParMultiDo(ConvertTimestamps)\","
                        +"                  \"shortName\": \"ParMultiDo(ConvertTimestamps)\","
                        +"                  \"enclosingNode\": \"Create.TimestampedValues/ParDo(ConvertTimestamps)\","
                        +"                  \"id\": \"Create.TimestampedValues/ParDo(ConvertTimestamps)/ParMultiDo(ConvertTimestamps)\""
                        +"                }"
                        +"              ]"
                        +"            }"
                        +"          ]"
                        +"        },"
                        +"        {"
                        +"          \"fullName\": \"Window.Into()\","
                        +"          \"shortName\": \"Window.Into()\","
                        +"          \"id\": \"Window.Into()\","
                        +"          \"enclosingNode\": \"OuterMostNode\","
                        +"          \"ChildNode\": ["
                        +"            {"
                        +"              \"fullName\": \"Window.Into()/Window.Assign\","
                        +"              \"shortName\": \"Window.Assign\","
                        +"              \"enclosingNode\": \"Window.Into()\","
                        +"              \"id\": \"Window.Into()/Window.Assign\""
                        +"            }"
                        +"          ]"
                        +"        },"
                        +"        {"
                        +"          \"fullName\": \"Combine.perKey(SumInteger)\","
                        +"          \"shortName\": \"Combine.perKey(SumInteger)\","
                        +"          \"id\": \"Combine.perKey(SumInteger)\","
                        +"          \"enclosingNode\": \"OuterMostNode\","
                        +"          \"ChildNode\": ["
                        +"            {"
                        +"              \"fullName\": \"Combine.perKey(SumInteger)/GroupByKey\","
                        +"              \"shortName\": \"GroupByKey\","
                        +"              \"enclosingNode\": \"Combine.perKey(SumInteger)\","
                        +"              \"id\": \"Combine.perKey(SumInteger)/GroupByKey\""
                        +"            },"
                        +"            {"
                        +"              \"fullName\": \"Combine.perKey(SumInteger)/Combine.GroupedValues\","
                        +"              \"shortName\": \"Combine.GroupedValues\","
                        +"              \"id\": \"Combine.perKey(SumInteger)/Combine.GroupedValues\","
                        +"              \"enclosingNode\": \"Combine.perKey(SumInteger)\","
                        +"              \"ChildNode\": ["
                        +"                {"
                        +"                  \"fullName\": \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)\","
                        +"                  \"shortName\": \"ParDo(Anonymous)\","
                        +"                  \"id\": \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)\","
                        +"                  \"enclosingNode\": \"Combine.perKey(SumInteger)/Combine.GroupedValues\","
                        +"                  \"ChildNode\": ["
                        +"                    {"
                        +"                      \"fullName\": \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)/ParMultiDo(Anonymous)\","
                        +"                      \"shortName\": \"ParMultiDo(Anonymous)\","
                        +"                      \"enclosingNode\": \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)\","
                        +"                      \"id\": \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)/ParMultiDo(Anonymous)\""
                        +"                    }"
                        +"                  ]"
                        +"                }"
                        +"              ]"
                        +"            }"
                        +"          ]"
                        +"        }"
                        +"      ]"
                        +"    }"
                        +"  ],"
                        +"  \"graphLinks\": ["
                        +"    {"
                        +"      \"from\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/Impulse\","
                        +"      \"to\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource)\","
                        +"      \"hashId\": \"402#bb20b45fd4d95138\""
                        +"    },"
                        +"    {"
                        +"      \"from\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource)\","
                        +"      \"to\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper)\","
                        +"      \"hashId\": \"402#3d93cb799b3970be\""
                        +"    },"
                        +"    {"
                        +"      \"from\": \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper)\","
                        +"      \"to\": \"Create.TimestampedValues/ParDo(ConvertTimestamps)/ParMultiDo(ConvertTimestamps)\","
                        +"      \"hashId\": \"402#a32dc9f64f1df03a\""
                        +"    },"
                        +"    {"
                        +"      \"from\": \"Create.TimestampedValues/ParDo(ConvertTimestamps)/ParMultiDo(ConvertTimestamps)\","
                        +"      \"to\": \"Window.Into()/Window.Assign\","
                        +"      \"hashId\": \"402#8ce970b71df42503\""
                        +"    },"
                        +"    {"
                        +"      \"from\": \"Window.Into()/Window.Assign\","
                        +"      \"to\": \"Combine.perKey(SumInteger)/GroupByKey\","
                        +"      \"hashId\": \"402#98f8ba3dc812a76d\""
                        +"    },"
                        +"    {"
                        +"      \"from\": \"Combine.perKey(SumInteger)/GroupByKey\","
                        +"      \"to\": \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)/ParMultiDo(Anonymous)\","
                        +"      \"hashId\": \"402#554dcd2b40b5f04b\""
                        +"    }"
                        +"  ]"
                        +"}").replaceAll("\\s+", ""),
                PipelineJsonRenderer.toJsonString(p).replaceAll(System.lineSeparator(), "").replaceAll("\\s+", ""));
    }
}
