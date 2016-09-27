package cz.seznam.euphoria.flink.testkit;

import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.operator.test.AllOperatorTest;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.junit.After;

public class FlinkOperatorTest extends AllOperatorTest {

  static String path;

  public FlinkOperatorTest() {
    super(s -> {
        path = "/tmp/.flink-test" + System.currentTimeMillis();
        try {
          RocksDBStateBackend backend = new RocksDBStateBackend("file://" + path);
          return new TestFlinkExecutor().setStateBackend(backend);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
    });
  }

  @After
  public void tearDown() throws IOException {
    if (path != null) {
      FileUtils.deleteQuietly(new File(path));
    }
  }

}
