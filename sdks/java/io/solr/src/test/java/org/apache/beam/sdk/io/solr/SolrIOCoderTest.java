package org.apache.beam.sdk.io.solr;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.io.CountingInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.UnownedInputStream;
import org.apache.beam.sdk.util.UnownedOutputStream;
import org.apache.solr.common.SolrDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test case for {@link SolrIO.SolrCoder}.
 */
@RunWith(JUnit4.class)
public class SolrIOCoderTest {

  private static final Coder<SolrDocument> TEST_CODER = SolrIO.SolrCoder.of();

  private static final List<SolrDocument> TEST_VALUES = new ArrayList<>();

  static {
    SolrDocument doc = new SolrDocument();
    doc.put("id", "1");
    doc.put("content", "wheel on the bus");
    doc.put("_version_", 1573597324260671488L);
    TEST_VALUES.add(doc);

    doc = new SolrDocument();
    doc.put("id", "2");
    doc.put("content", "goes round and round");
    doc.put("_version_", 1573597324260671489L);
    TEST_VALUES.add(doc);
  }

  @Test public void testDecodeEncodeEqual() throws Exception {
    for (SolrDocument value : TEST_VALUES) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      TEST_CODER.encode(value, new UnownedOutputStream(os));
      byte[] bytes = os.toByteArray();
      CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(bytes));
      SolrDocument decoded = TEST_CODER.decode(new UnownedInputStream(cis));
      assertThat("consumed bytes equal to encoded bytes", cis.getCount(),
          equalTo((long) bytes.length));
      assertThat(decoded.entrySet(), equalTo(value.entrySet()));
      assertThat(decoded.getChildDocuments(), equalTo(value.getChildDocuments()));
    }
  }
}
