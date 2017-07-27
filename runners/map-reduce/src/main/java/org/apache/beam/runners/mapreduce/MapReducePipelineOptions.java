package org.apache.beam.runners.mapreduce;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.Set;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * {@link PipelineOptions} for {@link MapReduceRunner}.
 */
public interface MapReducePipelineOptions extends PipelineOptions {

  /** Classes that are used as the boundary in the stack trace to find the callers class name. */
  Set<String> PIPELINE_OPTIONS_FACTORY_CLASSES = ImmutableSet.of(
      PipelineOptionsFactory.class.getName(),
      PipelineOptionsFactory.Builder.class.getName(),
      "org.apache.beam.sdk.options.ProxyInvocationHandler");


  @Description("The jar class of the user Beam program.")
  @Default.InstanceFactory(JarClassInstanceFactory.class)
  Class<?> getJarClass();
  void setJarClass(Class<?> jarClass);

  class JarClassInstanceFactory implements DefaultValueFactory<Class<?>> {
    @Override
    public Class<?> create(PipelineOptions options) {
      return findCallersClassName(options);
    }

    /**
     * Returns the simple name of the calling class using the current threads stack.
     */
    private static Class<?> findCallersClassName(PipelineOptions options) {
      Iterator<StackTraceElement> elements =
          Iterators.forArray(Thread.currentThread().getStackTrace());
      // First find the PipelineOptionsFactory/Builder class in the stack trace.
      while (elements.hasNext()) {
        StackTraceElement next = elements.next();
        if (PIPELINE_OPTIONS_FACTORY_CLASSES.contains(next.getClassName())) {
          break;
        }
      }
      // Then find the first instance after that is not the PipelineOptionsFactory/Builder class.
      while (elements.hasNext()) {
        StackTraceElement next = elements.next();
        if (!PIPELINE_OPTIONS_FACTORY_CLASSES.contains(next.getClassName())
            && !next.getClassName().contains("com.sun.proxy.$Proxy")
            && !next.getClassName().equals(options.getRunner().getName())) {
          try {
            return Class.forName(next.getClassName());
          } catch (ClassNotFoundException e) {
            break;
          }
        }
      }
      return null;
    }
  }
}
