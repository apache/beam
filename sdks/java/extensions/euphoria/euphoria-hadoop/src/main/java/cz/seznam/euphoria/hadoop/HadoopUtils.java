package cz.seznam.euphoria.hadoop;

import cz.seznam.euphoria.core.util.Settings;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.util.Map;

public class HadoopUtils {

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

  public static <T> T instantiateHadoopFormat(
          Class<? extends T> cls,
          Class<T> superType,
          Configuration conf)
          throws InstantiationException, IllegalAccessException
  {
    if (!superType.isAssignableFrom(cls)) {
      throw new IllegalStateException(cls + " is not a sub-class of " + superType);
    }

    T inst = cls.newInstance();
    if (inst instanceof Configurable) {
      ((Configurable) inst).setConf(conf);
    }

    return inst;
  }

  public static JobContext createJobContext(Configuration conf) {
    // TODO jobId uses some default hard-coded value
    return new JobContextImpl(conf, new JobID("", 0));
  }

  public static TaskAttemptContext createTaskContext(Configuration conf,
                                                     int taskNumber)
  {
    // TODO uses some default hard-coded values
    TaskAttemptID taskAttemptID = new TaskAttemptID(
            "0", // job tracker ID
            0, // job number,
            TaskType.REDUCE, // task type,
            taskNumber, // task ID
            0); // task attempt
    return new TaskAttemptContextImpl(conf, taskAttemptID);
  }
}
