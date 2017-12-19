package org.apache.beam.runners.core.metrics;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP Sink to push metrics in a POST HTTP request.
 */
//TODO extract in runner-extension module with DummySink as default impl for SPI dynamic injection
public class MetricsHttpSink extends MetricsSink<String> {
  private final String url;
//  private static final Logger LOG = LoggerFactory.getLogger(MetricsHttpSink.class);

  /**
   * @param url the URL of the endpoint
   */
  public MetricsHttpSink(String url) {
    this.url = url;
  }

  public String getEndpointUrl() {
    return url;
  }


  @Override protected MetricsSerializer<String> provideSerializer() {
    return new JsonMetricsSerializer();
  }

  @Override protected void writeSerializedMetrics(String metrics) throws Exception {
    HttpPost httpPost = new HttpPost(url);
    try {
      HttpEntity entity = new StringEntity(metrics);
      httpPost.setHeader("Content-Type", "application/json");
      httpPost.setEntity(entity);
      CloseableHttpClient httpclient = HttpClients.createDefault();
//      LOG.debug("Executing POST request: " + httpPost.getRequestLine());

      try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
//        LOG.debug("----------------------------------------");
//        LOG.debug("" + response.getStatusLine());
//        LOG.debug(EntityUtils.toString(response.getEntity()));
      }
    } catch (Exception ex) {
      // do not block the pipeline if a metrics push error occurs
      //      LOG.error(ex.getLocalizedMessage());
    }

  }
}
