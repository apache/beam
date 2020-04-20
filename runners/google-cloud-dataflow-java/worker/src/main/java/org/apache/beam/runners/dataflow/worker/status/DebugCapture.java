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
package org.apache.beam.runners.dataflow.worker.status;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.GetDebugConfigRequest;
import com.google.api.services.dataflow.model.GetDebugConfigResponse;
import com.google.api.services.dataflow.model.SendDebugCaptureRequest;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DebugCapture encapsulates a simple periodic sender for HTML pages to the debug capture service.
 * It is dynamically configured by the service.
 */
public class DebugCapture {
  private static final Logger LOG = LoggerFactory.getLogger(DebugCapture.class);
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();
  private static final String COMPONENT = "JavaHarness";
  // How often to refresh debug capture config.
  private static final long UPDATE_CONFIG_PERIOD_SEC = 60;
  // How often to check whether to send capture data.
  private static final long SEND_CAPTURE_PERIOD_SEC = 10;

  /** Config */
  public static class Config {
    @Key
    @JsonProperty("expire_timestamp_usec")
    public long expireTimestampUsec;

    @Key
    @JsonProperty("capture_frequency_usec")
    public long captureFrequencyUsec;
  }

  private static class Payload {
    @Key public List<Page> pages = new ArrayList<>();
  }

  private static class Page {
    public Page(String name, String content) {
      this.name = name;
      this.content = content;
    }

    // Implicitly read. Not a bug.
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    @Key
    public String name;

    // Implicitly read. Not a bug.
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    @Key
    public String content;
  }

  /** Interface for capturable components. */
  public interface Capturable {
    String pageName();

    void captureData(PrintWriter writer);
  }

  /** Manager */
  public static class Manager {
    private final Object lock = new Object();
    private final ObjectMapper mapper = new ObjectMapper();
    private String project, job, host, region;
    private Dataflow client = null;
    private ScheduledExecutorService executor = null;
    private Collection<Capturable> capturables;
    private boolean enabled;

    private long lastCaptureUsec = 0;
    @VisibleForTesting Config captureConfig = new Config();

    public Manager(DataflowWorkerHarnessOptions options, Collection<Capturable> capturables) {
      try {
        client = options.getDataflowClient();
      } catch (Exception e) {
        LOG.info("Failed to get dataflow client. Does not send debug capture data. ", e);
        return;
      }

      project = options.getProject();
      job = options.getJobId();
      host = options.getWorkerId();
      region = options.getRegion();
      this.capturables = capturables;
      enabled = !capturables.isEmpty();

      LOG.info("Created Debug Capture Manager");
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public void start() {
      if (!enabled) {
        LOG.info("Debug Capture Manager disabled");
        return;
      }

      executor = Executors.newScheduledThreadPool(2);
      executor.scheduleAtFixedRate(
          this::updateConfig, 0, UPDATE_CONFIG_PERIOD_SEC, TimeUnit.SECONDS);
      executor.scheduleAtFixedRate(
          this::maybeSendCapture, 0, SEND_CAPTURE_PERIOD_SEC, TimeUnit.SECONDS);

      LOG.info("Debug Capture Manager initialized");
    }

    public void stop() {
      if (executor != null) {
        executor.shutdown();
      }
    }

    @VisibleForTesting
    void updateConfig() {
      try {
        GetDebugConfigRequest request =
            new GetDebugConfigRequest().setComponentId(COMPONENT).setWorkerId(host);
        GetDebugConfigResponse response =
            this.client
                .projects()
                .locations()
                .jobs()
                .debug()
                .getConfig(project, region, job, request)
                .execute();
        Config config = mapper.readValue(response.getConfig(), Config.class);
        synchronized (lock) {
          captureConfig = config;
        }
      } catch (Exception e) {
        LOG.info("Does not update debug config", e);
      }
    }

    private boolean shouldSendCapture() {
      synchronized (lock) {
        long nowUsec = DateTime.now().getMillis() * 1000;
        if (captureConfig.expireTimestampUsec < nowUsec) {
          return false; // expired config
        }
        if (captureConfig.captureFrequencyUsec == 0) {
          return false; // disabled
        }
        return captureConfig.captureFrequencyUsec < (nowUsec - lastCaptureUsec);
      }
    }

    @VisibleForTesting
    void maybeSendCapture() {
      if (!shouldSendCapture()) {
        return;
      }

      try {
        Payload payload = new Payload();
        for (Capturable capturable : capturables) {
          StringWriter content = new StringWriter();
          PrintWriter writer = new PrintWriter(content);
          capturable.captureData(writer);
          writer.flush();
          payload.pages.add(new Page(capturable.pageName(), content.toString()));
        }

        SendDebugCaptureRequest request =
            new SendDebugCaptureRequest()
                .setComponentId(COMPONENT)
                .setWorkerId(host)
                .setData(JSON_FACTORY.toString(payload));
        this.client
            .projects()
            .locations()
            .jobs()
            .debug()
            .sendCapture(project, region, job, request)
            .execute();

        synchronized (lock) {
          lastCaptureUsec = DateTime.now().getMillis() * 1000;
        }

        LOG.debug("Sent Debug Capture payload");
      } catch (Exception e) {
        LOG.info("Does not send debug capture data", e);
      }
    }
  }
}
