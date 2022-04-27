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

package org.apache.flink.runners.metrics.datadog;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.datadog.DatadogHttpClient;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric Reporter for Datadog.
 *
 * <p>Variables in metrics scope will be sent to Datadog as tags.
 */
public class DatadogHttpReporter implements MetricReporter, Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatadogHttpReporter.class);
    private static final String HOST_VARIABLE = "<host>";

    private DatadogHttpClient client;
    private List<String> configTags;
    private int maxMetricsPerRequestValue;

    public static final String API_KEY = "apikey";
    public static final String PROXY_HOST = "proxyHost";
    public static final String PROXY_PORT = "proxyPort";
    public static final String DATA_CENTER = "dataCenter";
    public static final String TAGS = "tags";
    public static final String MAX_METRICS_PER_REQUEST = "maxMetricsPerRequest";

    @Override
    public void open(MetricConfig config) {
        String apiKey = config.getString(API_KEY, System.getenv("DD_API_KEY"));
        String proxyHost = config.getString(PROXY_HOST, null);
        Integer proxyPort = config.getInteger(PROXY_PORT, 8080);
        String rawDataCenter = config.getString(DATA_CENTER, "US");
        maxMetricsPerRequestValue = config.getInteger(MAX_METRICS_PER_REQUEST, 2000);
        DataCenter dataCenter = DataCenter.valueOf(rawDataCenter);
        String tags = config.getString(TAGS, "");

        client = new DatadogHttpClient(apiKey, proxyHost, proxyPort, dataCenter, true);

        configTags = getTagsFromConfig(tags);

        LOGGER.info(
                "Configured DatadogHttpReporter with {tags={}, proxyHost={}, proxyPort={}, dataCenter={}, maxMetricsPerRequest={}",
                tags,
                proxyHost,
                proxyPort,
                dataCenter,
                maxMetricsPerRequestValue);
    }

}
