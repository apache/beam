package org.apache.beam.runners.core.metrics;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/** HTTP Sink to push metrics in a POST HTTP request. */
// TODO extract in runner-extension module with DummySink as default impl for SPI dynamic injection
public class MetricsHttpSink extends MetricsSink<String> {
  private final String url;

  /** @param url the URL of the endpoint */
  public MetricsHttpSink(String url) {
    this.url = url;
  }

  @Override
  protected MetricsSerializer<String> provideSerializer() {
    return new JsonMetricsSerializer();
  }

  @Override
  protected void writeSerializedMetrics(String metrics) throws Exception {
    HttpPost httpPost = new HttpPost(url);
    HttpEntity entity = new StringEntity(metrics);
    httpPost.setHeader("Content-Type", "application/json");
    httpPost.setEntity(entity);
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      try (CloseableHttpResponse execute = httpClient.execute(httpPost)) {}
    }
  }
}
