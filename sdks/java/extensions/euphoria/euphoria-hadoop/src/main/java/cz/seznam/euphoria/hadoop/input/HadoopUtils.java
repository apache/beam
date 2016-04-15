package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.util.Settings;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.util.Map;

class HadoopUtils {

  /**
   * Initialize Hadoop {@link Configuration} from Euphoria specific
   * {@link Settings} instance.
   */
  public static Configuration createConfiguration(Settings settings) {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> c : settings.getAll().entrySet()) {
      conf.set(c.getKey(), c.getValue());
    }

    return conf;
  }

  public static InputFormat<?, ?> instantiateHadoopFormat(
          Class<? extends InputFormat> cls,
          Configuration conf)
          throws InstantiationException, IllegalAccessException
  {
    InputFormat<?,?> inst = cls.newInstance();
    if (inst instanceof Configurable) {
      ((Configurable) inst).setConf(conf);
    }

    return inst;
  }

  public static JobContext createJobContext(Configuration conf) {
    // TODO jobId uses some default hard-coded value
    return new JobContextImpl(conf, new JobID("", 0));
  }

  public static TaskAttemptContext createTaskContext(Configuration conf) {
    // TODO uses some default hard-coded values
    int taskNumber = 0;
    int taskAttemptNumber = 0;
    TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt_"
            + 0
            + "_0000_r_"
            + String.format("%" + (6 - Integer.toString(taskNumber).length()) + "s", " ")
            .replace(" ", "0") + Integer.toString(taskNumber) + "_" + taskAttemptNumber);
    return new TaskAttemptContextImpl(conf, taskAttemptID);
  }
}
