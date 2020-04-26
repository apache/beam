import com.google.common.collect.Lists;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.publishing.InfluxDBPublisher;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TTestClas {
    @Test
    public void doSth() throws IOException {
        final List<NamedTestResult> list = new ArrayList<>();
        final String testId = UUID.randomUUID().toString();
        final String measurement = "java_ssie_tylko_kotlin";
        final List<String> metryki = new ArrayList<>(Arrays.asList("czas", "rozmiar", "kolejna_metryka", "i_cos_tam_jeszcze"));
        for (String bla : metryki) {
            list.add(NamedTestResult.create(
                    testId,
                    new Date().toString(),
                    bla,
                    new Random().nextDouble()
            ));
        }
        InfluxDBPublisher.publishWithSettings(list, null);
    }
}
