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
package org.apache.beam.sdk.options;

import static java.util.Locale.ROOT;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.deser.impl.MethodProperty;
import com.fasterxml.jackson.databind.deser.impl.TypeWrappedDeserializer;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.AnnotationCollector;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.introspect.TypeResolutionContext;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.type.TypeBindings;
import com.fasterxml.jackson.databind.util.SimpleBeanPropertyDefinition;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.apache.beam.model.jobmanagement.v1.JobApi.PipelineOptionDescriptor;
import org.apache.beam.model.jobmanagement.v1.JobApi.PipelineOptionType;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSortedSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Ordering;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.RowSortedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.SortedSetMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeMultimap;
import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constructs a {@link PipelineOptions} or any derived interface that is composable to any other
 * derived interface of {@link PipelineOptions} via the {@link PipelineOptions#as} method. Being
 * able to compose one derived interface of {@link PipelineOptions} to another has the following
 * restrictions:
 *
 * <ul>
 *   <li>Any property with the same name must have the same return type for all derived interfaces
 *       of {@link PipelineOptions}.
 *   <li>Every bean property of any interface derived from {@link PipelineOptions} must have a
 *       getter and setter method.
 *   <li>Every method must conform to being a getter or setter for a JavaBean.
 *   <li>The derived interface of {@link PipelineOptions} must be composable with every interface
 *       registered with this factory.
 * </ul>
 *
 * <p>See the <a
 * href="http://www.oracle.com/technetwork/java/javase/documentation/spec-136004.html">JavaBeans
 * specification</a> for more details as to what constitutes a property.
 */
@SuppressWarnings({
  "keyfor",
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class PipelineOptionsFactory {
  /**
   * Creates and returns an object that implements {@link PipelineOptions}. This sets the {@link
   * ApplicationNameOptions#getAppName() "appName"} to the calling {@link Class#getSimpleName()
   * classes simple name}.
   *
   * @return An object that implements {@link PipelineOptions}.
   */
  public static PipelineOptions create() {
    return new Builder().as(PipelineOptions.class);
  }

  /**
   * Creates and returns an object that implements {@code <T>}. This sets the {@link
   * ApplicationNameOptions#getAppName() "appName"} to the calling {@link Class#getSimpleName()
   * classes simple name}.
   *
   * <p>Note that {@code <T>} must be composable with every registered interface with this factory.
   * See {@link PipelineOptionsFactory.Cache#validateWellFormed(Class, Set)} for more details.
   *
   * @return An object that implements {@code <T>}.
   */
  public static <T extends PipelineOptions> T as(Class<T> klass) {
    return new Builder().as(klass);
  }

  /**
   * Sets the command line arguments to parse when constructing the {@link PipelineOptions}.
   *
   * <p>Example GNU style command line arguments:
   *
   * <pre>
   *   --project=MyProject (simple property, will set the "project" property to "MyProject")
   *   --readOnly=true (for boolean properties, will set the "readOnly" property to "true")
   *   --readOnly (shorthand for boolean properties, will set the "readOnly" property to "true")
   *   --x=1 --x=2 --x=3 (list style simple property, will set the "x" property to [1, 2, 3])
   *   --x=1,2,3 (shorthand list style simple property, will set the "x" property to [1, 2, 3])
   *   --complexObject='{"key1":"value1",...} (JSON format for all other complex types)
   * </pre>
   *
   * <p>Simple properties are able to bound to {@link String}, {@link Class}, enums and Java
   * primitives {@code boolean}, {@code byte}, {@code short}, {@code int}, {@code long}, {@code
   * float}, {@code double} and their primitive wrapper classes.
   *
   * <p>Simple list style properties are able to be bound to {@code boolean[]}, {@code char[]},
   * {@code short[]}, {@code int[]}, {@code long[]}, {@code float[]}, {@code double[]}, {@code
   * Class[]}, enum arrays, {@code String[]}, and {@code List<String>}.
   *
   * <p>JSON format is required for all other types.
   *
   * <p>By default, strict parsing is enabled and arguments must conform to be either {@code
   * --booleanArgName} or {@code --argName=argValue}. Strict parsing can be disabled with {@link
   * Builder#withoutStrictParsing()}. Empty or null arguments will be ignored whether or not strict
   * parsing is enabled.
   *
   * <p>Help information can be output to {@link System#out} by specifying {@code --help} as an
   * argument. After help is printed, the application will exit. Specifying only {@code --help} will
   * print out the list of {@link PipelineOptionsFactory#getRegisteredOptions() registered options}
   * by invoking {@link PipelineOptionsFactory#printHelp(PrintStream)}. Specifying {@code
   * --help=PipelineOptionsClassName} will print out detailed usage information about the
   * specifically requested PipelineOptions by invoking {@link
   * PipelineOptionsFactory#printHelp(PrintStream, Class)}.
   */
  public static Builder fromArgs(String... args) {
    return new Builder().fromArgs(args);
  }

  /**
   * After creation we will validate that {@code <T>} conforms to all the validation criteria. See
   * {@link PipelineOptionsValidator#validate(Class, PipelineOptions)} for more details about
   * validation.
   */
  public Builder withValidation() {
    return new Builder().withValidation();
  }

  /** A fluent {@link PipelineOptions} builder. */
  public static class Builder {
    private final String defaultAppName;
    private final String[] args;
    private final boolean validation;
    private final boolean strictParsing;
    private final boolean isCli;

    // Do not allow direct instantiation
    private Builder() {
      this(new String[0], false, true, false);
    }

    private Builder(String[] args, boolean validation, boolean strictParsing, boolean isCli) {
      this.defaultAppName = findCallersClassName();
      this.args = args;
      this.validation = validation;
      this.strictParsing = strictParsing;
      this.isCli = isCli;
    }

    /**
     * Sets the command line arguments to parse when constructing the {@link PipelineOptions}.
     *
     * <p>Example GNU style command line arguments:
     *
     * <pre>
     *   --project=MyProject (simple property, will set the "project" property to "MyProject")
     *   --readOnly=true (for boolean properties, will set the "readOnly" property to "true")
     *   --readOnly (shorthand for boolean properties, will set the "readOnly" property to "true")
     *   --x=1 --x=2 --x=3 (list style simple property, will set the "x" property to [1, 2, 3])
     *   --x=1,2,3 (shorthand list style simple property, will set the "x" property to [1, 2, 3])
     *   --complexObject='{"key1":"value1",...} (JSON format for all other complex types)
     * </pre>
     *
     * <p>Simple properties are able to bound to {@link String}, {@link Class}, enums and Java
     * primitives {@code boolean}, {@code byte}, {@code short}, {@code int}, {@code long}, {@code
     * float}, {@code double} and their primitive wrapper classes.
     *
     * <p>Simple list style properties are able to be bound to {@code boolean[]}, {@code char[]},
     * {@code short[]}, {@code int[]}, {@code long[]}, {@code float[]}, {@code double[]}, {@code
     * Class[]}, enum arrays, {@code String[]}, and {@code List<String>}.
     *
     * <p>JSON format is required for all other types.
     *
     * <p>By default, strict parsing is enabled and arguments must conform to be either {@code
     * --booleanArgName} or {@code --argName=argValue}. Strict parsing can be disabled with {@link
     * Builder#withoutStrictParsing()}. Empty or null arguments will be ignored whether or not
     * strict parsing is enabled.
     *
     * <p>Help information can be output to {@link System#out} by specifying {@code --help} as an
     * argument. After help is printed, the application will exit. Specifying only {@code --help}
     * will print out the list of {@link PipelineOptionsFactory#getRegisteredOptions() registered
     * options} by invoking {@link PipelineOptionsFactory#printHelp(PrintStream)}. Specifying {@code
     * --help=PipelineOptionsClassName} will print out detailed usage information about the
     * specifically requested PipelineOptions by invoking {@link
     * PipelineOptionsFactory#printHelp(PrintStream, Class)}.
     */
    public Builder fromArgs(String... args) {
      checkNotNull(args, "Arguments should not be null.");
      return new Builder(args, validation, strictParsing, true);
    }

    /**
     * After creation we will validate that {@link PipelineOptions} conforms to all the validation
     * criteria from {@code <T>}. See {@link PipelineOptionsValidator#validate(Class,
     * PipelineOptions)} for more details about validation.
     */
    public Builder withValidation() {
      return new Builder(args, true, strictParsing, isCli);
    }

    /**
     * During parsing of the arguments, we will skip over improperly formatted and unknown
     * arguments.
     */
    public Builder withoutStrictParsing() {
      return new Builder(args, validation, false, isCli);
    }

    /**
     * Creates and returns an object that implements {@link PipelineOptions} using the values
     * configured on this builder during construction.
     *
     * @return An object that implements {@link PipelineOptions}.
     */
    public PipelineOptions create() {
      return as(PipelineOptions.class);
    }

    /**
     * Creates and returns an object that implements {@code <T>} using the values configured on this
     * builder during construction.
     *
     * <p>Note that {@code <T>} must be composable with every registered interface with this
     * factory. See {@link PipelineOptionsFactory.Cache#validateWellFormed(Class)} for more details.
     *
     * @return An object that implements {@code <T>}.
     */
    public <T extends PipelineOptions> T as(Class<T> klass) {
      Map<String, Object> initialOptions = Maps.newHashMap();

      // Attempt to parse the arguments into the set of initial options to use
      if (args != null) {
        ListMultimap<String, String> options = parseCommandLine(args, strictParsing);
        LOG.debug("Provided Arguments: {}", options);
        printHelpUsageAndExitIfNeeded(options, System.out, true /* exit */);
        initialOptions = parseObjects(klass, options, strictParsing);
      }

      // Create our proxy
      ProxyInvocationHandler handler = new ProxyInvocationHandler(initialOptions);
      T t = handler.as(klass);

      // Set the application name to the default if none was set.
      ApplicationNameOptions appNameOptions = t.as(ApplicationNameOptions.class);
      if (appNameOptions.getAppName() == null) {
        appNameOptions.setAppName(defaultAppName);
      }

      // Ensure the options id has been populated either by the user using the command line
      // or by the default value factory.
      t.getOptionsId();

      if (validation) {
        if (isCli) {
          PipelineOptionsValidator.validateCli(klass, t);
        } else {
          PipelineOptionsValidator.validate(klass, t);
        }
      }
      return t;
    }
  }

  /**
   * Determines whether the generic {@code --help} was requested or help was requested for a
   * specific class and invokes the appropriate {@link
   * PipelineOptionsFactory#printHelp(PrintStream)} and {@link
   * PipelineOptionsFactory#printHelp(PrintStream, Class)} variant. Prints to the specified {@link
   * PrintStream}, and exits if requested.
   *
   * <p>Visible for testing. {@code printStream} and {@code exit} used for testing.
   */
  @SuppressWarnings("unchecked")
  static boolean printHelpUsageAndExitIfNeeded(
      ListMultimap<String, String> options, PrintStream printStream, boolean exit) {
    if (options.containsKey("help")) {
      final String helpOption = Iterables.getOnlyElement(options.get("help"));

      // Print the generic help if only --help was specified.
      if (Boolean.TRUE.toString().equals(helpOption)) {
        printHelp(printStream);
        if (exit) {
          System.exit(0);
        } else {
          return true;
        }
      }

      // Otherwise attempt to print the specific help option.
      try {
        Class<?> klass = Class.forName(helpOption, true, ReflectHelpers.findClassLoader());
        if (!PipelineOptions.class.isAssignableFrom(klass)) {
          throw new ClassNotFoundException("PipelineOptions of type " + klass + " not found.");
        }
        printHelp(printStream, (Class<? extends PipelineOptions>) klass);
      } catch (ClassNotFoundException e) {
        // If we didn't find an exact match, look for any that match the class name.
        Iterable<Class<? extends PipelineOptions>> matches =
            getRegisteredOptions().stream()
                .filter(
                    input -> {
                      if (helpOption.contains(".")) {
                        return input.getName().endsWith(helpOption);
                      } else {
                        return input.getSimpleName().equals(helpOption);
                      }
                    })
                .collect(Collectors.toList());
        try {
          printHelp(printStream, Iterables.getOnlyElement(matches));
        } catch (NoSuchElementException exception) {
          printStream.format("Unable to find option %s.%n", helpOption);
          printHelp(printStream);
        } catch (IllegalArgumentException exception) {
          printStream.format(
              "Multiple matches found for %s: %s.%n",
              helpOption,
              StreamSupport.stream(matches.spliterator(), false)
                  .map(Class::getName)
                  .collect(Collectors.toList()));
          printHelp(printStream);
        }
      }
      if (exit) {
        System.exit(0);
      } else {
        return true;
      }
    }
    return false;
  }

  /** Returns the simple name of the calling class using the current threads stack. */
  private static String findCallersClassName() {
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
      if (!PIPELINE_OPTIONS_FACTORY_CLASSES.contains(next.getClassName())) {
        try {
          return Class.forName(next.getClassName(), true, ReflectHelpers.findClassLoader())
              .getSimpleName();
        } catch (ClassNotFoundException e) {
          break;
        }
      }
    }

    return "unknown";
  }

  /**
   * Stores the generated proxyClass and its respective {@link BeanInfo} object.
   *
   * @param <T> The type of the proxyClass.
   */
  static class Registration<T extends PipelineOptions> {
    private final Class<T> proxyClass;
    private final List<PropertyDescriptor> propertyDescriptors;

    public Registration(Class<T> proxyClass, List<PropertyDescriptor> beanInfo) {
      this.proxyClass = proxyClass;
      this.propertyDescriptors = beanInfo;
    }

    List<PropertyDescriptor> getPropertyDescriptors() {
      return propertyDescriptors;
    }

    Class<T> getProxyClass() {
      return proxyClass;
    }
  }

  private static final ImmutableSet<Class<?>> SIMPLE_TYPES =
      ImmutableSet.<Class<?>>builder()
          .add(boolean.class)
          .add(Boolean.class)
          .add(char.class)
          .add(Character.class)
          .add(short.class)
          .add(Short.class)
          .add(int.class)
          .add(Integer.class)
          .add(long.class)
          .add(Long.class)
          .add(float.class)
          .add(Float.class)
          .add(double.class)
          .add(Double.class)
          .add(String.class)
          .add(Class.class)
          .build();
  private static final Logger LOG = LoggerFactory.getLogger(PipelineOptionsFactory.class);

  @SuppressWarnings("rawtypes")
  private static final Class<?>[] EMPTY_CLASS_ARRAY = new Class[0];

  static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  private static final DefaultDeserializationContext DESERIALIZATION_CONTEXT =
      new DefaultDeserializationContext.Impl(MAPPER.getDeserializationContext().getFactory())
          .createInstance(
              MAPPER.getDeserializationConfig(),
              new TokenBuffer(MAPPER, false).asParser(),
              new InjectableValues.Std());

  static final DefaultSerializerProvider SERIALIZER_PROVIDER =
      new DefaultSerializerProvider.Impl()
          .createInstance(MAPPER.getSerializationConfig(), MAPPER.getSerializerFactory());

  /** Classes that are used as the boundary in the stack trace to find the callers class name. */
  private static final ImmutableSet<String> PIPELINE_OPTIONS_FACTORY_CLASSES =
      ImmutableSet.of(PipelineOptionsFactory.class.getName(), Builder.class.getName());

  /** Methods that are ignored when validating the proxy class. */
  private static final Set<Method> IGNORED_METHODS;

  /** Ensure all classloader or volatile data are contained in a single reference. */
  static final AtomicReference<Cache> CACHE = new AtomicReference<>();

  /** The width at which options should be output. */
  private static final int TERMINAL_WIDTH = 80;

  static {
    try {
      IGNORED_METHODS =
          ImmutableSet.<Method>builder()
              .add(Object.class.getMethod("getClass"))
              .add(Object.class.getMethod("wait"))
              .add(Object.class.getMethod("wait", long.class))
              .add(Object.class.getMethod("wait", long.class, int.class))
              .add(Object.class.getMethod("notify"))
              .add(Object.class.getMethod("notifyAll"))
              .add(Proxy.class.getMethod("getInvocationHandler", Object.class))
              .build();
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.error("Unable to find expected method", e);
      throw new ExceptionInInitializerError(e);
    }
    resetCache();
  }

  /**
   * This registers the interface with this factory. This interface must conform to the following
   * restrictions:
   *
   * <ul>
   *   <li>Any property with the same name must have the same return type for all derived interfaces
   *       of {@link PipelineOptions}.
   *   <li>Every bean property of any interface derived from {@link PipelineOptions} must have a
   *       getter and setter method.
   *   <li>Every method must conform to being a getter or setter for a JavaBean.
   *   <li>The derived interface of {@link PipelineOptions} must be composable with every interface
   *       registered with this factory.
   * </ul>
   *
   * @param iface The interface object to manually register.
   */
  public static synchronized void register(Class<? extends PipelineOptions> iface) {
    CACHE.get().register(iface);
  }

  /**
   * Resets the set of interfaces registered with this factory to the default state.
   *
   * <p>IMPORTANT: correct usage of this method requires appropriate synchronization beyond the
   * scope of this method.
   *
   * @see PipelineOptionsFactory#register(Class)
   * @see Cache#Cache()
   */
  public static synchronized void resetCache() {
    CACHE.set(new Cache());
  }

  public static Set<Class<? extends PipelineOptions>> getRegisteredOptions() {
    return Collections.unmodifiableSet(CACHE.get().registeredOptions);
  }

  /**
   * Outputs the set of registered options with the PipelineOptionsFactory with a description for
   * each one if available to the output stream. This output is pretty printed and meant to be human
   * readable. This method will attempt to format its output to be compatible with a terminal
   * window.
   */
  public static void printHelp(PrintStream out) {
    checkNotNull(out);
    out.println("The set of registered options are:");
    Set<Class<? extends PipelineOptions>> sortedOptions =
        new TreeSet<>(ClassNameComparator.INSTANCE);
    sortedOptions.addAll(CACHE.get().registeredOptions);
    for (Class<? extends PipelineOptions> kls : sortedOptions) {
      out.format("  %s%n", kls.getName());
    }
    out.format(
        "%nUse --help=<OptionsName> for detailed help. For example:%n"
            + "  --help=DataflowPipelineOptions <short names valid for registered options>%n"
            + "  --help=org.apache.beam.runners.dataflow.options.DataflowPipelineOptions%n");
  }

  /**
   * Outputs the set of options available to be set for the passed in {@link PipelineOptions}
   * interface. The output is in a human readable format. The format is:
   *
   * <pre>
   * OptionGroup:
   *     ... option group description ...
   *
   *  --option1={@code <type>} or list of valid enum choices
   *     Default: value (if available, see {@link Default})
   *     ... option description ... (if available, see {@link Description})
   *     Required groups (if available, see {@link Required})
   *  --option2={@code <type>} or list of valid enum choices
   *     Default: value (if available, see {@link Default})
   *     ... option description ... (if available, see {@link Description})
   *     Required groups (if available, see {@link Required})
   * </pre>
   *
   * This method will attempt to format its output to be compatible with a terminal window.
   */
  public static void printHelp(PrintStream out, Class<? extends PipelineOptions> iface) {
    checkNotNull(out);
    checkNotNull(iface);
    CACHE.get().validateWellFormed(iface);

    Set<PipelineOptionSpec> properties = PipelineOptionsReflector.getOptionSpecs(iface, true);

    RowSortedTable<Class<?>, String, Method> ifacePropGetterTable =
        TreeBasedTable.create(ClassNameComparator.INSTANCE, Ordering.natural());
    for (PipelineOptionSpec prop : properties) {
      ifacePropGetterTable.put(prop.getDefiningInterface(), prop.getName(), prop.getGetterMethod());
    }

    for (Map.Entry<Class<?>, Map<String, Method>> ifaceToPropertyMap :
        ifacePropGetterTable.rowMap().entrySet()) {
      Class<?> currentIface = ifaceToPropertyMap.getKey();
      Map<String, Method> propertyNamesToGetters = ifaceToPropertyMap.getValue();

      SortedSetMultimap<String, String> requiredGroupNameToProperties =
          getRequiredGroupNamesToProperties(propertyNamesToGetters);

      out.format("%s:%n", currentIface.getName());
      Description ifaceDescription = currentIface.getAnnotation(Description.class);
      if (ifaceDescription != null && ifaceDescription.value() != null) {
        prettyPrintDescription(out, ifaceDescription);
      }

      out.println();

      List<@KeyFor("propertyNamesToGetters") String> lists =
          Lists.newArrayList(propertyNamesToGetters.keySet());
      lists.sort(String.CASE_INSENSITIVE_ORDER);
      for (String propertyName : lists) {
        Method method = propertyNamesToGetters.get(propertyName);
        String printableType = method.getReturnType().getSimpleName();
        if (method.getReturnType().isEnum()) {
          Object @Nullable [] enumConstants = method.getReturnType().getEnumConstants();
          assert enumConstants != null : "@AssumeAssertion(nullness): checked that it is an enum";
          printableType = Joiner.on(" | ").join(method.getReturnType().getEnumConstants());
        }
        out.format("  --%s=<%s>%n", propertyName, printableType);
        Optional<String> defaultValue = getDefaultValueFromAnnotation(method);
        if (defaultValue.isPresent()) {
          out.format("    Default: %s%n", defaultValue.get());
        }
        @Nullable Description methodDescription = method.getAnnotation(Description.class);
        if (methodDescription != null && methodDescription.value() != null) {
          prettyPrintDescription(out, methodDescription);
        }
        prettyPrintRequiredGroups(
            out, method.getAnnotation(Validation.Required.class), requiredGroupNameToProperties);
      }
      out.println();
    }
  }

  private static final Set<Class<?>> JSON_INTEGER_TYPES =
      Sets.newHashSet(
          short.class,
          Short.class,
          int.class,
          Integer.class,
          long.class,
          Long.class,
          BigInteger.class);

  private static final Set<Class<?>> JSON_NUMBER_TYPES =
      Sets.newHashSet(
          float.class, Float.class, double.class, Double.class, java.math.BigDecimal.class);

  /**
   * Outputs the set of options available to be set for the passed in {@link PipelineOptions}
   * interfaces. The output for consumption of the job service client.
   */
  public static List<PipelineOptionDescriptor> describe(
      Set<Class<? extends PipelineOptions>> ifaces) {
    checkNotNull(ifaces);
    List<PipelineOptionDescriptor> result = new ArrayList<>();
    Set<Method> seenMethods = Sets.newHashSet();

    for (Class<? extends PipelineOptions> iface : ifaces) {
      CACHE.get().validateWellFormed(iface);

      Set<PipelineOptionSpec> properties = PipelineOptionsReflector.getOptionSpecs(iface, false);

      RowSortedTable<Class<?>, String, Method> ifacePropGetterTable =
          TreeBasedTable.create(ClassNameComparator.INSTANCE, Ordering.natural());
      for (PipelineOptionSpec prop : properties) {
        ifacePropGetterTable.put(
            prop.getDefiningInterface(), prop.getName(), prop.getGetterMethod());
      }

      for (Map.Entry<Class<?>, Map<String, Method>> ifaceToPropertyMap :
          ifacePropGetterTable.rowMap().entrySet()) {
        Class<?> currentIface = ifaceToPropertyMap.getKey();
        Map<String, Method> propertyNamesToGetters = ifaceToPropertyMap.getValue();

        List<@KeyFor("propertyNamesToGetters") String> lists =
            Lists.newArrayList(propertyNamesToGetters.keySet());
        lists.sort(String.CASE_INSENSITIVE_ORDER);
        for (String propertyName : lists) {
          Method method = propertyNamesToGetters.get(propertyName);
          if (!seenMethods.add(method)) {
            continue;
          }
          Class<?> returnType = method.getReturnType();
          PipelineOptionType.Enum optionType = PipelineOptionType.Enum.STRING;
          if (JSON_INTEGER_TYPES.contains(returnType)) {
            optionType = PipelineOptionType.Enum.INTEGER;
          } else if (JSON_NUMBER_TYPES.contains(returnType)) {
            optionType = PipelineOptionType.Enum.NUMBER;
          } else if (returnType == boolean.class || returnType == Boolean.class) {
            optionType = PipelineOptionType.Enum.BOOLEAN;
          } else if (List.class.isAssignableFrom(returnType)) {
            optionType = PipelineOptionType.Enum.ARRAY;
          }
          String optionName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, propertyName);
          Description description = method.getAnnotation(Description.class);
          PipelineOptionDescriptor.Builder builder =
              PipelineOptionDescriptor.newBuilder()
                  .setName(optionName)
                  .setType(optionType)
                  .setGroup(currentIface.getName());
          Optional<String> defaultValue = getDefaultValueFromAnnotation(method);
          if (defaultValue.isPresent()) {
            builder.setDefaultValue(defaultValue.get());
          }
          if (description != null) {
            builder.setDescription(description.value());
          }
          result.add(builder.build());
        }
      }
    }
    return result;
  }

  /**
   * Output the requirement groups that the property is a member of, including all properties that
   * satisfy the group requirement, breaking up long lines on white space characters and attempting
   * to honor a line limit of {@code TERMINAL_WIDTH}.
   */
  private static void prettyPrintRequiredGroups(
      PrintStream out,
      Required annotation,
      SortedSetMultimap<String, String> requiredGroupNameToProperties) {
    if (annotation == null || annotation.groups() == null) {
      return;
    }
    for (String group : annotation.groups()) {
      SortedSet<String> groupMembers = requiredGroupNameToProperties.get(group);
      String requirement;
      if (groupMembers.size() == 1) {
        requirement = Iterables.getOnlyElement(groupMembers) + " is required.";
      } else {
        requirement = "At least one of " + groupMembers + " is required";
      }
      terminalPrettyPrint(out, requirement.split("\\s+"));
    }
  }

  /**
   * Outputs the value of the description, breaking up long lines on white space characters and
   * attempting to honor a line limit of {@code TERMINAL_WIDTH}.
   */
  private static void prettyPrintDescription(PrintStream out, Description description) {
    String[] words = description.value().split("\\s+");
    terminalPrettyPrint(out, words);
  }

  private static void terminalPrettyPrint(PrintStream out, String[] words) {
    final String spacing = "   ";

    if (words.length == 0) {
      return;
    }

    out.print(spacing);
    int lineLength = spacing.length();
    for (int i = 0; i < words.length; ++i) {
      out.print(" ");
      out.print(words[i]);
      lineLength += 1 + words[i].length();

      // If the next word takes us over the terminal width, then goto the next line.
      if (i + 1 != words.length && words[i + 1].length() + lineLength + 1 > TERMINAL_WIDTH) {
        out.println();
        out.print(spacing);
        lineLength = spacing.length();
      }
    }
    out.println();
  }

  /** Returns a string representation of the {@link Default} value on the passed in method. */
  private static Optional<String> getDefaultValueFromAnnotation(Method method) {
    for (Annotation annotation : method.getAnnotations()) {
      if (annotation instanceof Default.Class) {
        return Optional.of(((Default.Class) annotation).value().getSimpleName());
      } else if (annotation instanceof Default.String) {
        return Optional.of(((Default.String) annotation).value());
      } else if (annotation instanceof Default.Boolean) {
        return Optional.of(Boolean.toString(((Default.Boolean) annotation).value()));
      } else if (annotation instanceof Default.Character) {
        return Optional.of(Character.toString(((Default.Character) annotation).value()));
      } else if (annotation instanceof Default.Byte) {
        return Optional.of(Byte.toString(((Default.Byte) annotation).value()));
      } else if (annotation instanceof Default.Short) {
        return Optional.of(Short.toString(((Default.Short) annotation).value()));
      } else if (annotation instanceof Default.Integer) {
        return Optional.of(Integer.toString(((Default.Integer) annotation).value()));
      } else if (annotation instanceof Default.Long) {
        return Optional.of(Long.toString(((Default.Long) annotation).value()));
      } else if (annotation instanceof Default.Float) {
        return Optional.of(Float.toString(((Default.Float) annotation).value()));
      } else if (annotation instanceof Default.Double) {
        return Optional.of(Double.toString(((Default.Double) annotation).value()));
      } else if (annotation instanceof Default.Enum) {
        return Optional.of(((Default.Enum) annotation).value());
      } else if (annotation instanceof Default.InstanceFactory) {
        return Optional.of(((Default.InstanceFactory) annotation).value().getSimpleName());
      }
    }
    return Optional.absent();
  }

  static Map<String, Class<? extends PipelineRunner<?>>> getRegisteredRunners() {
    return CACHE.get().supportedPipelineRunners;
  }

  /**
   * This method is meant to emulate the behavior of {@link Introspector#getBeanInfo(Class, int)} to
   * construct the list of {@link PropertyDescriptor}.
   *
   * <p>TODO: Swap back to using Introspector once the proxy class issue with AppEngine is resolved.
   */
  private static List<PropertyDescriptor> getPropertyDescriptors(Set<Method> methods)
      throws IntrospectionException {
    SortedMap<String, Method> propertyNamesToGetters = new TreeMap<>();
    for (Map.Entry<String, Method> entry :
        PipelineOptionsReflector.getPropertyNamesToGetters(methods).entries()) {
      propertyNamesToGetters.put(entry.getKey(), entry.getValue());
    }

    List<PropertyDescriptor> descriptors = Lists.newArrayList();
    List<TypeMismatch> mismatches = new ArrayList<>();
    Set<String> usedDescriptors = Sets.newHashSet();
    /*
     * Add all the getter/setter pairs to the list of descriptors removing the getter once
     * it has been paired up.
     */
    for (Method method : methods) {
      String methodName = method.getName();
      if (!methodName.startsWith("set")
          || method.getParameterTypes().length != 1
          || method.getReturnType() != void.class) {
        continue;
      }
      String propertyName = Introspector.decapitalize(methodName.substring(3));
      Method getterMethod = propertyNamesToGetters.remove(propertyName);

      // Validate that the getter and setter property types are the same.
      if (getterMethod != null) {
        Type getterPropertyType = getterMethod.getGenericReturnType();
        Type setterPropertyType = method.getGenericParameterTypes()[0];
        if (!getterPropertyType.equals(setterPropertyType)) {
          TypeMismatch mismatch = new TypeMismatch();
          mismatch.propertyName = propertyName;
          mismatch.getterPropertyType = getterPropertyType;
          mismatch.setterPropertyType = setterPropertyType;
          mismatches.add(mismatch);
          continue;
        }
      }
      // Properties can appear multiple times with subclasses, and we don't
      // want to add a bad entry if we have already added a good one (with both
      // getter and setter).
      if (!usedDescriptors.contains(propertyName)) {
        descriptors.add(new PropertyDescriptor(propertyName, getterMethod, method));
        usedDescriptors.add(propertyName);
      }
    }
    throwForTypeMismatches(mismatches);

    // Add the remaining getters with missing setters.
    for (Map.Entry<String, Method> getterToMethod : propertyNamesToGetters.entrySet()) {
      descriptors.add(
          new PropertyDescriptor(getterToMethod.getKey(), getterToMethod.getValue(), null));
    }
    return descriptors;
  }

  private static class TypeMismatch {
    private String propertyName;
    private Type getterPropertyType;
    private Type setterPropertyType;
  }

  private static void throwForTypeMismatches(List<TypeMismatch> mismatches) {
    if (mismatches.size() == 1) {
      TypeMismatch mismatch = mismatches.get(0);
      throw new IllegalArgumentException(
          String.format(
              "Type mismatch between getter and setter methods for property [%s]. "
                  + "Getter is of type [%s] whereas setter is of type [%s].",
              mismatch.propertyName, mismatch.getterPropertyType, mismatch.setterPropertyType));
    } else if (mismatches.size() > 1) {
      StringBuilder builder =
          new StringBuilder("Type mismatches between getters and setters detected:");
      for (TypeMismatch mismatch : mismatches) {
        builder.append(
            String.format(
                "%n  - Property [%s]: Getter is of type [%s] whereas setter is of type [%s].",
                mismatch.propertyName,
                mismatch.getterPropertyType.toString(),
                mismatch.setterPropertyType.toString()));
      }
      throw new IllegalArgumentException(builder.toString());
    }
  }

  /**
   * Returns a map of required groups of arguments to the properties that satisfy the requirement.
   */
  private static SortedSetMultimap<String, String> getRequiredGroupNamesToProperties(
      Map<String, Method> propertyNamesToGetters) {
    SortedSetMultimap<String, String> result = TreeMultimap.create();
    for (Map.Entry<String, Method> propertyEntry : propertyNamesToGetters.entrySet()) {
      Required requiredAnnotation =
          propertyEntry.getValue().getAnnotation(Validation.Required.class);
      if (requiredAnnotation != null) {
        for (String groupName : requiredAnnotation.groups()) {
          result.put(groupName, propertyEntry.getKey());
        }
      }
    }
    return result;
  }

  /**
   * Validates that a given class conforms to the following properties:
   *
   * <ul>
   *   <li>Any method with the same name must have the same return type for all derived interfaces
   *       of {@link PipelineOptions}.
   *   <li>Every bean property of any interface derived from {@link PipelineOptions} must have a
   *       getter and setter method.
   *   <li>Every method must conform to being a getter or setter for a JavaBean.
   *   <li>Only getters may be annotated with {@link JsonIgnore @JsonIgnore}.
   *   <li>If any getter is annotated with {@link JsonIgnore @JsonIgnore}, then all getters for this
   *       property must be annotated with {@link JsonIgnore @JsonIgnore}.
   * </ul>
   *
   * @param iface The interface to validate.
   * @param validatedPipelineOptionsInterfaces The set of validated pipeline options interfaces to
   *     validate against.
   * @param klass The proxy class representing the interface.
   * @return A list of {@link PropertyDescriptor}s representing all valid bean properties of {@code
   *     iface}.
   * @throws IntrospectionException if invalid property descriptors.
   */
  private static List<PropertyDescriptor> validateClass(
      Class<? extends PipelineOptions> iface,
      Set<Class<? extends PipelineOptions>> validatedPipelineOptionsInterfaces,
      Class<? extends PipelineOptions> klass)
      throws IntrospectionException {

    checkArgument(
        Modifier.isPublic(iface.getModifiers()),
        "Please mark non-public interface %s as public. The JVM requires that "
            + "all non-public interfaces to be in the same package which will prevent the "
            + "PipelineOptions proxy class to implement all of the interfaces.",
        iface.getName());

    // Verify that there are no methods with the same name with two different return types.
    validateReturnType(iface);

    SortedSet<Method> allInterfaceMethods =
        Stream.concat(
                StreamSupport.stream(
                    ReflectHelpers.getClosureOfMethodsOnInterfaces(
                            validatedPipelineOptionsInterfaces)
                        .spliterator(),
                    false),
                StreamSupport.stream(
                    ReflectHelpers.getClosureOfMethodsOnInterface(iface).spliterator(), false))
            .filter(input -> !input.isSynthetic())
            .filter(input1 -> !Modifier.isStatic(input1.getModifiers()))
            .collect(ImmutableSortedSet.toImmutableSortedSet(MethodComparator.INSTANCE));

    List<PropertyDescriptor> descriptors = getPropertyDescriptors(allInterfaceMethods);

    // Verify that all method annotations are valid.
    validateMethodAnnotations(allInterfaceMethods, descriptors);

    // Verify that each property has a matching read and write method.
    validateGettersSetters(iface, descriptors);

    // Verify all methods are bean methods or known methods.
    validateMethodsAreEitherBeanMethodOrKnownMethod(iface, klass, descriptors);

    return descriptors;
  }

  /**
   * Validates that any method with the same name must have the same return type for all derived
   * interfaces of {@link PipelineOptions}.
   *
   * @param iface The interface to validate.
   */
  private static void validateReturnType(Class<? extends PipelineOptions> iface) {
    Iterable<Method> interfaceMethods =
        StreamSupport.stream(
                ReflectHelpers.getClosureOfMethodsOnInterface(iface).spliterator(), false)
            .filter(input -> !input.isSynthetic())
            .collect(ImmutableSortedSet.toImmutableSortedSet(MethodComparator.INSTANCE));
    SortedSetMultimap<Method, Method> methodNameToMethodMap =
        TreeMultimap.create(MethodNameComparator.INSTANCE, MethodComparator.INSTANCE);
    for (Method method : interfaceMethods) {
      methodNameToMethodMap.put(method, method);
    }
    List<MultipleDefinitions> multipleDefinitions = Lists.newArrayList();
    for (Map.Entry<Method, Collection<Method>> entry : methodNameToMethodMap.asMap().entrySet()) {
      Set<Class<?>> returnTypes =
          entry.getValue().stream()
              .map(ReturnTypeFetchingFunction.INSTANCE)
              .collect(Collectors.toSet());
      SortedSet<Method> collidingMethods =
          StreamSupport.stream(entry.getValue().spliterator(), false)
              .collect(ImmutableSortedSet.toImmutableSortedSet(MethodComparator.INSTANCE));
      if (returnTypes.size() > 1) {
        MultipleDefinitions defs = new MultipleDefinitions();
        defs.method = entry.getKey();
        defs.collidingMethods = collidingMethods;
        multipleDefinitions.add(defs);
      }
    }
    throwForMultipleDefinitions(iface, multipleDefinitions);
  }

  /**
   * Validates that a given class conforms to the following properties:
   *
   * <ul>
   *   <li>Only getters may be annotated with {@link JsonIgnore @JsonIgnore}.
   *   <li>If any getter is annotated with {@link JsonIgnore @JsonIgnore}, then all getters for this
   *       property must be annotated with {@link JsonIgnore @JsonIgnore}.
   * </ul>
   *
   * @param allInterfaceMethods All interface methods that derive from {@link PipelineOptions}.
   * @param descriptors The list of {@link PropertyDescriptor}s representing all valid bean
   *     properties of {@code iface}.
   */
  private static void validateMethodAnnotations(
      SortedSet<Method> allInterfaceMethods, List<PropertyDescriptor> descriptors) {
    SortedSetMultimap<Method, Method> methodNameToAllMethodMap =
        TreeMultimap.create(MethodNameComparator.INSTANCE, MethodComparator.INSTANCE);
    for (Method method : allInterfaceMethods) {
      methodNameToAllMethodMap.put(method, method);
    }

    // Verify that there is no getter with a mixed @JsonIgnore annotation.
    validateGettersHaveConsistentAnnotation(
        methodNameToAllMethodMap, descriptors, AnnotationPredicates.JSON_IGNORE);

    // Verify that there is no getter with a mixed @Default annotation.
    validateGettersHaveConsistentAnnotation(
        methodNameToAllMethodMap, descriptors, AnnotationPredicates.DEFAULT_VALUE);

    // Verify that there is no getter with a mixed @JsonDeserialize annotation.
    validateGettersHaveConsistentAnnotation(
        methodNameToAllMethodMap, descriptors, AnnotationPredicates.JSON_DESERIALIZE);

    // Verify that there is no getter with a mixed @JsonSerialize annotation.
    validateGettersHaveConsistentAnnotation(
        methodNameToAllMethodMap, descriptors, AnnotationPredicates.JSON_SERIALIZE);

    // Verify that if a method has either @JsonSerialize or @JsonDeserialize then it has both.
    validateMethodsHaveBothJsonSerializeAndDeserialize(descriptors);

    // Verify that no setter has @JsonIgnore.
    validateSettersDoNotHaveAnnotation(
        methodNameToAllMethodMap, descriptors, AnnotationPredicates.JSON_IGNORE);

    // Verify that no setter has @Default.
    validateSettersDoNotHaveAnnotation(
        methodNameToAllMethodMap, descriptors, AnnotationPredicates.DEFAULT_VALUE);

    // Verify that no setter has @JsonDeserialize.
    validateSettersDoNotHaveAnnotation(
        methodNameToAllMethodMap, descriptors, AnnotationPredicates.JSON_DESERIALIZE);

    // Verify that no setter has @JsonSerialize.
    validateSettersDoNotHaveAnnotation(
        methodNameToAllMethodMap, descriptors, AnnotationPredicates.JSON_SERIALIZE);
  }

  /** Validates that getters don't have mixed annotation. */
  private static void validateGettersHaveConsistentAnnotation(
      SortedSetMultimap<Method, Method> methodNameToAllMethodMap,
      List<PropertyDescriptor> descriptors,
      final AnnotationPredicates annotationPredicates) {
    List<InconsistentlyAnnotatedGetters> inconsistentlyAnnotatedGetters = new ArrayList<>();
    for (final PropertyDescriptor descriptor : descriptors) {
      if (descriptor.getReadMethod() == null
          || IGNORED_METHODS.contains(descriptor.getReadMethod())) {
        continue;
      }

      SortedSet<Method> getters = methodNameToAllMethodMap.get(descriptor.getReadMethod());
      SortedSet<Method> gettersWithTheAnnotation =
          Sets.filter(getters, annotationPredicates.forMethod);
      Set<Annotation> distinctAnnotations =
          gettersWithTheAnnotation.stream()
              .flatMap(method -> Arrays.stream(method.getAnnotations()))
              .filter(annotationPredicates.forAnnotation)
              .collect(Collectors.toSet());

      if (distinctAnnotations.size() > 1) {
        throw new IllegalArgumentException(
            String.format(
                "Property [%s] is marked with contradictory annotations. Found [%s].",
                descriptor.getName(),
                gettersWithTheAnnotation.stream()
                    .flatMap(
                        method ->
                            Arrays.stream(method.getAnnotations())
                                .filter(annotationPredicates.forAnnotation)
                                .map(
                                    annotation ->
                                        String.format(
                                            "[%s on %s]",
                                            ReflectHelpers.formatAnnotation(annotation),
                                            ReflectHelpers.formatMethodWithClass(method))))
                    .collect(Collectors.joining(", "))));
      }

      Iterable<String> getterClassNames =
          getters.stream()
              .map(MethodToDeclaringClassFunction.INSTANCE)
              .map(Class::getName)
              .collect(Collectors.toList());
      Iterable<String> gettersWithTheAnnotationClassNames =
          gettersWithTheAnnotation.stream()
              .map(MethodToDeclaringClassFunction.INSTANCE)
              .map(Class::getName)
              .collect(Collectors.toList());

      if (!(gettersWithTheAnnotation.isEmpty()
          || getters.size() == gettersWithTheAnnotation.size())) {
        InconsistentlyAnnotatedGetters err = new InconsistentlyAnnotatedGetters();
        err.descriptor = descriptor;
        err.getterClassNames = getterClassNames;
        err.gettersWithTheAnnotationClassNames = gettersWithTheAnnotationClassNames;
        inconsistentlyAnnotatedGetters.add(err);
      }
    }
    throwForGettersWithInconsistentAnnotation(
        inconsistentlyAnnotatedGetters, annotationPredicates.annotationClass);
  }

  /** Validates that setters don't have the given annotation. */
  private static void validateSettersDoNotHaveAnnotation(
      SortedSetMultimap<Method, Method> methodNameToAllMethodMap,
      List<PropertyDescriptor> descriptors,
      AnnotationPredicates annotationPredicates) {
    List<AnnotatedSetter> annotatedSetters = new ArrayList<>();
    for (PropertyDescriptor descriptor : descriptors) {
      if (descriptor.getWriteMethod() == null
          || IGNORED_METHODS.contains(descriptor.getWriteMethod())) {
        continue;
      }
      SortedSet<Method> settersWithTheAnnotation =
          Sets.filter(
              methodNameToAllMethodMap.get(descriptor.getWriteMethod()),
              annotationPredicates.forMethod);

      Iterable<String> settersWithTheAnnotationClassNames =
          settersWithTheAnnotation.stream()
              .map(MethodToDeclaringClassFunction.INSTANCE)
              .map(Class::getName)
              .collect(Collectors.toList());

      if (!settersWithTheAnnotation.isEmpty()) {
        AnnotatedSetter annotated = new AnnotatedSetter();
        annotated.descriptor = descriptor;
        annotated.settersWithTheAnnotationClassNames = settersWithTheAnnotationClassNames;
        annotatedSetters.add(annotated);
      }
    }
    throwForSettersWithTheAnnotation(annotatedSetters, annotationPredicates.annotationClass);
  }

  /**
   * Validates that every bean property of the given interface must have both a getter and setter.
   *
   * @param iface The interface to validate.
   * @param descriptors The list of {@link PropertyDescriptor}s representing all valid bean
   *     properties of {@code iface}.
   */
  private static void validateGettersSetters(
      Class<? extends PipelineOptions> iface, List<PropertyDescriptor> descriptors) {
    List<MissingBeanMethod> missingBeanMethods = new ArrayList<>();
    for (PropertyDescriptor propertyDescriptor : descriptors) {
      if (!(IGNORED_METHODS.contains(propertyDescriptor.getWriteMethod())
          || propertyDescriptor.getReadMethod() != null)) {
        MissingBeanMethod method = new MissingBeanMethod();
        method.property = propertyDescriptor;
        method.methodType = "getter";
        missingBeanMethods.add(method);
        continue;
      }
      if (!(IGNORED_METHODS.contains(propertyDescriptor.getReadMethod())
          || propertyDescriptor.getWriteMethod() != null)) {
        MissingBeanMethod method = new MissingBeanMethod();
        method.property = propertyDescriptor;
        method.methodType = "setter";
        missingBeanMethods.add(method);
      }
    }
    throwForMissingBeanMethod(iface, missingBeanMethods);
  }

  /**
   * Validates that every non-static or synthetic method is either a known method such as {@link
   * PipelineOptions#as} or a bean property.
   *
   * @param iface The interface to validate.
   * @param klass The proxy class representing the interface.
   */
  private static void validateMethodsAreEitherBeanMethodOrKnownMethod(
      Class<? extends PipelineOptions> iface,
      Class<? extends PipelineOptions> klass,
      List<PropertyDescriptor> descriptors) {
    Set<Method> knownMethods = Sets.newHashSet(IGNORED_METHODS);
    // Ignore synthetic methods
    for (Method method : klass.getMethods()) {
      if (Modifier.isStatic(method.getModifiers()) || method.isSynthetic()) {
        knownMethods.add(method);
      }
    }
    // Ignore methods on the base PipelineOptions interface.
    try {
      knownMethods.add(iface.getMethod("as", Class.class));
      knownMethods.add(iface.getMethod("outputRuntimeOptions"));
      knownMethods.add(iface.getMethod("revision"));
      knownMethods.add(iface.getMethod("populateDisplayData", DisplayData.Builder.class));
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(e);
    }
    for (PropertyDescriptor descriptor : descriptors) {
      knownMethods.add(descriptor.getReadMethod());
      knownMethods.add(descriptor.getWriteMethod());
    }
    final Set<String> knownMethodsNames = Sets.newHashSet();
    for (Method method : knownMethods) {
      knownMethodsNames.add(method.getName());
    }

    // Verify that no additional methods are on an interface that aren't a bean property.
    // Because methods can have multiple declarations, we do a name-based comparison
    // here to prevent false positives.
    SortedSet<Method> unknownMethods = new TreeSet<>(MethodComparator.INSTANCE);
    unknownMethods.addAll(
        Sets.filter(
            Sets.difference(Sets.newHashSet(iface.getMethods()), knownMethods),
            Predicates.and(
                input1 -> !input1.isSynthetic(),
                input -> !knownMethodsNames.contains(input.getName()),
                input11 -> !Modifier.isStatic(input11.getModifiers()))));
    checkArgument(
        unknownMethods.isEmpty(),
        "Methods [%s] on [%s] do not conform to being bean properties.",
        unknownMethods.stream().map(ReflectHelpers::formatMethod).collect(Collectors.joining(",")),
        iface.getName());
  }

  private static void validateMethodsHaveBothJsonSerializeAndDeserialize(
      List<PropertyDescriptor> descriptors) {
    List<InconsistentJsonSerializeAndDeserializeAnnotation> inconsistentMethods =
        Lists.newArrayList();
    for (final PropertyDescriptor descriptor : descriptors) {
      Method readMethod = descriptor.getReadMethod();
      if (readMethod == null || IGNORED_METHODS.contains(descriptor.getReadMethod())) {
        continue;
      }

      boolean hasJsonSerialize = AnnotationPredicates.JSON_SERIALIZE.forMethod.apply(readMethod);
      boolean hasJsonDeserialize =
          AnnotationPredicates.JSON_DESERIALIZE.forMethod.apply(readMethod);
      if (hasJsonSerialize ^ hasJsonDeserialize) {
        InconsistentJsonSerializeAndDeserializeAnnotation inconsistentAnnotation =
            new InconsistentJsonSerializeAndDeserializeAnnotation();
        inconsistentAnnotation.property = descriptor;
        inconsistentAnnotation.hasJsonDeserializeAttribute = hasJsonDeserialize;
        inconsistentMethods.add(inconsistentAnnotation);
      }
    }

    throwForInconsistentJsonSerializeAndDeserializeAnnotation(inconsistentMethods);
  }

  private static void checkInheritedFrom(
      Class<?> checkClass, Class fromClass, Set<Class<?>> nonPipelineOptions) {
    if (checkClass.equals(fromClass)) {
      return;
    }

    if (checkClass.getInterfaces().length == 0) {
      nonPipelineOptions.add(checkClass);
      return;
    }

    for (Class<?> klass : checkClass.getInterfaces()) {
      checkInheritedFrom(klass, fromClass, nonPipelineOptions);
    }
  }

  private static void throwNonPipelineOptions(
      Class<?> klass, Set<Class<?>> nonPipelineOptionsClasses) {
    StringBuilder errorBuilder =
        new StringBuilder(
            String.format(
                "All inherited interfaces of [%s] should inherit from the PipelineOptions"
                    + " interface. The following inherited interfaces do not:",
                klass.getName()));

    for (Class<?> invalidKlass : nonPipelineOptionsClasses) {
      errorBuilder.append(String.format("%n - %s", invalidKlass.getName()));
    }
    throw new IllegalArgumentException(errorBuilder.toString());
  }

  private static void validateInheritedInterfacesExtendPipelineOptions(Class<?> klass) {
    Set<Class<?>> nonPipelineOptionsClasses = new LinkedHashSet<>();
    checkInheritedFrom(klass, PipelineOptions.class, nonPipelineOptionsClasses);

    if (!nonPipelineOptionsClasses.isEmpty()) {
      throwNonPipelineOptions(klass, nonPipelineOptionsClasses);
    }
  }

  private static class MultipleDefinitions {
    private Method method;
    private SortedSet<Method> collidingMethods;
  }

  private static void throwForMultipleDefinitions(
      Class<? extends PipelineOptions> iface, List<MultipleDefinitions> definitions) {
    if (definitions.size() == 1) {
      MultipleDefinitions errDef = definitions.get(0);
      throw new IllegalArgumentException(
          String.format(
              "Method [%s] has multiple definitions %s with different return types for [%s].",
              errDef.method.getName(), errDef.collidingMethods, iface.getName()));
    } else if (definitions.size() > 1) {
      StringBuilder errorBuilder =
          new StringBuilder(
              String.format(
                  "Interface [%s] has Methods with multiple definitions with different return"
                      + " types:",
                  iface.getName()));
      for (MultipleDefinitions errDef : definitions) {
        errorBuilder.append(
            String.format(
                "%n  - Method [%s] has multiple definitions %s",
                errDef.method.getName(), errDef.collidingMethods));
      }
      throw new IllegalArgumentException(errorBuilder.toString());
    }
  }

  private static class InconsistentlyAnnotatedGetters {
    PropertyDescriptor descriptor;
    Iterable<String> getterClassNames;
    Iterable<String> gettersWithTheAnnotationClassNames;
  }

  private static void throwForGettersWithInconsistentAnnotation(
      List<InconsistentlyAnnotatedGetters> getters, Class<? extends Annotation> annotationClass) {
    if (getters.size() == 1) {
      InconsistentlyAnnotatedGetters getter = getters.get(0);
      throw new IllegalArgumentException(
          String.format(
              "Expected getter for property [%s] to be marked with @%s on all %s, "
                  + "found only on %s",
              getter.descriptor.getName(),
              annotationClass.getSimpleName(),
              getter.getterClassNames,
              getter.gettersWithTheAnnotationClassNames));
    } else if (getters.size() > 1) {
      StringBuilder errorBuilder =
          new StringBuilder(
              String.format(
                  "Property getters are inconsistently marked with @%s:",
                  annotationClass.getSimpleName()));
      for (InconsistentlyAnnotatedGetters getter : getters) {
        errorBuilder.append(
            String.format(
                "%n  - Expected for property [%s] to be marked on all %s, " + "found only on %s",
                getter.descriptor.getName(),
                getter.getterClassNames,
                getter.gettersWithTheAnnotationClassNames));
      }
      throw new IllegalArgumentException(errorBuilder.toString());
    }
  }

  private static class AnnotatedSetter {
    PropertyDescriptor descriptor;
    Iterable<String> settersWithTheAnnotationClassNames;
  }

  private static void throwForSettersWithTheAnnotation(
      List<AnnotatedSetter> setters, Class<? extends Annotation> annotationClass) {
    if (setters.size() == 1) {
      AnnotatedSetter setter = setters.get(0);
      throw new IllegalArgumentException(
          String.format(
              "Expected setter for property [%s] to not be marked with @%s on %s",
              setter.descriptor.getName(),
              annotationClass.getSimpleName(),
              setter.settersWithTheAnnotationClassNames));
    } else if (setters.size() > 1) {
      StringBuilder builder =
          new StringBuilder(
              String.format("Found setters marked with @%s:", annotationClass.getSimpleName()));
      for (AnnotatedSetter setter : setters) {
        builder.append(
            String.format(
                "%n  - Setter for property [%s] should not be marked with @%s on %s",
                setter.descriptor.getName(),
                annotationClass.getSimpleName(),
                setter.settersWithTheAnnotationClassNames));
      }
      throw new IllegalArgumentException(builder.toString());
    }
  }

  private static class MissingBeanMethod {
    String methodType;
    PropertyDescriptor property;
  }

  private static void throwForMissingBeanMethod(
      Class<? extends PipelineOptions> iface, List<MissingBeanMethod> missingBeanMethods) {
    if (missingBeanMethods.size() == 1) {
      MissingBeanMethod missingBeanMethod = missingBeanMethods.get(0);
      throw new IllegalArgumentException(
          String.format(
              "Expected %s for property [%s] of type [%s] on [%s].",
              missingBeanMethod.methodType,
              missingBeanMethod.property.getName(),
              missingBeanMethod.property.getPropertyType().getName(),
              iface.getName()));
    } else if (missingBeanMethods.size() > 1) {
      StringBuilder builder =
          new StringBuilder(
              String.format("Found missing property methods on [%s]:", iface.getName()));
      for (MissingBeanMethod method : missingBeanMethods) {
        builder.append(
            String.format(
                "%n  - Expected %s for property [%s] of type [%s]",
                method.methodType,
                method.property.getName(),
                method.property.getPropertyType().getName()));
      }
      throw new IllegalArgumentException(builder.toString());
    }
  }

  private static class InconsistentJsonSerializeAndDeserializeAnnotation {
    PropertyDescriptor property;
    boolean hasJsonDeserializeAttribute;
  }

  private static void throwForInconsistentJsonSerializeAndDeserializeAnnotation(
      List<InconsistentJsonSerializeAndDeserializeAnnotation> inconsistentAnnotations)
      throws IllegalArgumentException {
    if (inconsistentAnnotations.isEmpty()) {
      return;
    }

    StringBuilder builder =
        new StringBuilder(
            "Found incorrectly annotated property methods, if a method is annotated with either @JsonSerialize or @JsonDeserialize then it must be annotated with both.");

    for (InconsistentJsonSerializeAndDeserializeAnnotation annotation : inconsistentAnnotations) {
      String presentAnnotation;
      if (annotation.hasJsonDeserializeAttribute) {
        presentAnnotation = "JsonDeserialize";
      } else {
        presentAnnotation = "JsonSerialize";
      }
      builder.append(
          String.format(
              "%n  - Property [%s] had only @%s",
              annotation.property.getName(), presentAnnotation));
    }

    throw new IllegalArgumentException(builder.toString());
  }

  /** A {@link Comparator} that uses the classes name to compare them. */
  private static class ClassNameComparator implements Comparator<Class<?>> {
    static final ClassNameComparator INSTANCE = new ClassNameComparator();

    @Override
    public int compare(Class<?> o1, Class<?> o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }

  /** A {@link Comparator} that uses the generic method signature to sort them. */
  private static class MethodComparator implements Comparator<Method> {
    static final MethodComparator INSTANCE = new MethodComparator();

    @Override
    public int compare(Method o1, Method o2) {
      return o1.toGenericString().compareTo(o2.toGenericString());
    }
  }

  /** A {@link Comparator} that uses the methods name to compare them. */
  static class MethodNameComparator implements Comparator<Method> {
    static final MethodNameComparator INSTANCE = new MethodNameComparator();

    @Override
    public int compare(Method o1, Method o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }

  /** A {@link Function} that gets the method's return type. */
  private static class ReturnTypeFetchingFunction implements Function<Method, Class<?>> {
    static final ReturnTypeFetchingFunction INSTANCE = new ReturnTypeFetchingFunction();

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public Class<?> apply(@Nonnull Method input) {
      return input.getReturnType();
    }
  }

  /** A {@link Function} with returns the declaring class for the method. */
  private static class MethodToDeclaringClassFunction implements Function<Method, Class<?>> {
    static final MethodToDeclaringClassFunction INSTANCE = new MethodToDeclaringClassFunction();

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public Class<?> apply(@Nonnull Method input) {
      return input.getDeclaringClass();
    }
  }

  /**
   * A {@link Predicate} that returns true if the method is annotated with {@code annotationClass}.
   */
  static class AnnotationPredicates {
    static final AnnotationPredicates JSON_IGNORE =
        new AnnotationPredicates(
            JsonIgnore.class,
            input -> JsonIgnore.class.equals(input.annotationType()),
            input -> input.isAnnotationPresent(JsonIgnore.class));

    private static final Set<Class<?>> DEFAULT_ANNOTATION_CLASSES =
        Sets.newHashSet(
            Arrays.stream(Default.class.getDeclaredClasses())
                .filter(Class::isAnnotation)
                .collect(Collectors.toSet()));

    static final AnnotationPredicates DEFAULT_VALUE =
        new AnnotationPredicates(
            Default.class,
            input -> DEFAULT_ANNOTATION_CLASSES.contains(input.annotationType()),
            input -> {
              for (Annotation annotation : input.getAnnotations()) {
                if (DEFAULT_ANNOTATION_CLASSES.contains(annotation.annotationType())) {
                  return true;
                }
              }
              return false;
            });

    static final AnnotationPredicates JSON_DESERIALIZE =
        new AnnotationPredicates(
            JsonDeserialize.class,
            input -> JsonDeserialize.class.equals(input.annotationType()),
            input -> input.isAnnotationPresent(JsonDeserialize.class));

    static final AnnotationPredicates JSON_SERIALIZE =
        new AnnotationPredicates(
            JsonSerialize.class,
            input -> JsonSerialize.class.equals(input.annotationType()),
            input -> input.isAnnotationPresent(JsonSerialize.class));

    final Class<? extends Annotation> annotationClass;
    final Predicate<Annotation> forAnnotation;
    final Predicate<Method> forMethod;

    AnnotationPredicates(
        Class<? extends Annotation> annotationClass,
        Predicate<Annotation> forAnnotation,
        Predicate<Method> forMethod) {
      this.annotationClass = annotationClass;
      this.forAnnotation = forAnnotation;
      this.forMethod = forMethod;
    }
  }

  /**
   * Splits string arguments based upon expected pattern of --argName=value.
   *
   * <p>Example GNU style command line arguments:
   *
   * <pre>
   *   --project=MyProject (simple property, will set the "project" property to "MyProject")
   *   --readOnly=true (for boolean properties, will set the "readOnly" property to "true")
   *   --readOnly (shorthand for boolean properties, will set the "readOnly" property to "true")
   *   --x=1 --x=2 --x=3 (list style simple property, will set the "x" property to [1, 2, 3])
   *   --x=1,2,3 (shorthand list style simple property, will set the "x" property to [1, 2, 3])
   *   --complexObject='{"key1":"value1",...} (JSON format for all other complex types)
   * </pre>
   *
   * <p>Simple properties are able to bound to {@link String}, {@link Class}, enums and Java
   * primitives {@code boolean}, {@code byte}, {@code short}, {@code int}, {@code long}, {@code
   * float}, {@code double} and their primitive wrapper classes.
   *
   * <p>Simple list style properties are able to be bound to {@code boolean[]}, {@code char[]},
   * {@code short[]}, {@code int[]}, {@code long[]}, {@code float[]}, {@code double[]}, {@code
   * Class[]}, enum arrays, {@code String[]}, and {@code List<String>}.
   *
   * <p>JSON format is required for all other types.
   *
   * <p>If strict parsing is enabled, options must start with '--', and not have an empty argument
   * name or value based upon the positioning of the '='. Empty or null arguments will be ignored
   * whether or not strict parsing is enabled.
   */
  private static ListMultimap<String, String> parseCommandLine(
      String[] args, boolean strictParsing) {
    ImmutableListMultimap.Builder<String, String> builder = ImmutableListMultimap.builder();
    for (String arg : args) {
      if (Strings.isNullOrEmpty(arg)) {
        continue;
      }
      try {
        checkArgument(arg.startsWith("--"), "Argument '%s' does not begin with '--'", arg);
        int index = arg.indexOf('=');
        // Make sure that '=' isn't the first character after '--' or the last character
        checkArgument(
            index != 2, "Argument '%s' starts with '--=', empty argument name not allowed", arg);
        if (index > 0) {
          builder.put(arg.substring(2, index), arg.substring(index + 1, arg.length()));
        } else {
          builder.put(arg.substring(2), "true");
        }
      } catch (IllegalArgumentException e) {
        if (strictParsing) {
          throw e;
        } else {
          LOG.warn(
              "Strict parsing is disabled, ignoring option '{}' because {}", arg, e.getMessage());
        }
      }
    }
    return builder.build();
  }

  private static BeanProperty createBeanProperty(Method method) {
    AnnotationCollector ac = AnnotationCollector.emptyCollector();
    for (Annotation ann : method.getAnnotations()) {
      ac = ac.addOrOverride(ann);
    }

    AnnotatedMethod annotatedMethod =
        new AnnotatedMethod(
            new TypeResolutionContext.Basic(MAPPER.getTypeFactory(), TypeBindings.emptyBindings()),
            method,
            ac.asAnnotationMap(),
            null);

    BeanPropertyDefinition propDef =
        SimpleBeanPropertyDefinition.construct(MAPPER.getDeserializationConfig(), annotatedMethod);

    JavaType type = MAPPER.constructType(method.getGenericReturnType());

    try {
      return new MethodProperty(
          propDef,
          type,
          MAPPER.getDeserializationConfig().findTypeDeserializer(type),
          annotatedMethod.getAllAnnotations(),
          annotatedMethod);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }

  private static JsonDeserializer<Object> computeDeserializerForMethod(Method method) {
    try {
      BeanProperty prop = createBeanProperty(method);
      AnnotatedMember annotatedMethod = prop.getMember();

      DefaultDeserializationContext context = DESERIALIZATION_CONTEXT.copy();
      Object maybeDeserializerClass =
          context.getAnnotationIntrospector().findDeserializer(annotatedMethod);

      JsonDeserializer<Object> jsonDeserializer =
          context.deserializerInstance(annotatedMethod, maybeDeserializerClass);

      if (jsonDeserializer == null) {
        jsonDeserializer = context.findContextualValueDeserializer(prop.getType(), prop);
      }

      TypeDeserializer typeDeserializer =
          context.getFactory().findTypeDeserializer(context.getConfig(), prop.getType());
      if (typeDeserializer != null) {
        jsonDeserializer = new TypeWrappedDeserializer(typeDeserializer, jsonDeserializer);
      }

      return jsonDeserializer;
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }

  private static Optional<JsonSerializer<Object>> computeCustomSerializerForMethod(Method method) {
    try {
      BeanProperty prop = createBeanProperty(method);
      AnnotatedMember annotatedMethod = prop.getMember();

      Object maybeSerializerClass =
          SERIALIZER_PROVIDER.getAnnotationIntrospector().findSerializer(annotatedMethod);

      return Optional.fromNullable(
          SERIALIZER_PROVIDER.serializerInstance(annotatedMethod, maybeSerializerClass));
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get a {@link JsonDeserializer} for a given method. If the method is annotated with {@link
   * JsonDeserialize} the specified deserializer from the annotation is returned, otherwise the
   * default is returned.
   */
  private static JsonDeserializer<Object> getDeserializerForMethod(Method method) {
    return CACHE
        .get()
        .deserializerCache
        .computeIfAbsent(method, PipelineOptionsFactory::computeDeserializerForMethod);
  }

  /**
   * Get a {@link JsonSerializer} for a given method. If the method is annotated with {@link
   * JsonDeserialize} the specified serializer from the annotation is returned, otherwise null is
   * returned.
   */
  static @Nullable JsonSerializer<Object> getCustomSerializerForMethod(Method method) {
    return CACHE
        .get()
        .serializerCache
        .computeIfAbsent(method, PipelineOptionsFactory::computeCustomSerializerForMethod)
        .orNull();
  }

  static Object deserializeNode(JsonNode node, Method method) throws IOException {
    if (node.isNull()) {
      return null;
    }

    JsonParser parser = new TreeTraversingParser(node, MAPPER);
    parser.nextToken();

    JsonDeserializer<Object> jsonDeserializer = getDeserializerForMethod(method);
    return jsonDeserializer.deserialize(parser, DESERIALIZATION_CONTEXT.copy());
  }

  /**
   * Attempt to parse an input string into an instance of `type` using an {@link ObjectMapper}.
   *
   * <p>If the getter method is annotated with {@link
   * com.fasterxml.jackson.databind.annotation.JsonDeserialize} the specified deserializer will be
   * used, otherwise the default ObjectMapper deserialization strategy is used.
   *
   * <p>Parsing is attempted twice, once with the raw string value. If that attempt fails, another
   * attempt is made by wrapping the value in quotes so that it is interpreted as a JSON string.
   */
  private static Object tryParseObject(String value, Method method) throws IOException {

    JsonNode tree;
    try {
      tree = MAPPER.readTree(value);
    } catch (JsonParseException e) {
      // try again, quoting the input string if it wasn't already
      if (!(value.startsWith("\"") && value.endsWith("\""))) {
        try {
          tree = MAPPER.readTree("\"" + value + "\"");
        } catch (JsonParseException inner) {
          // rethrow the original exception rather the one thrown from the fallback attempt
          throw e;
        }
      } else {
        throw e;
      }
    }

    return deserializeNode(tree, method);
  }

  /**
   * Using the parsed string arguments, we convert the strings to the expected return type of the
   * methods that are found on the passed-in class.
   *
   * <p>For any return type that is expected to be an array or a collection, we further split up
   * each string on ','.
   *
   * <p>We special case the "runner" option. It is mapped to the class of the {@link PipelineRunner}
   * based off of the {@link PipelineRunner PipelineRunners} simple class name. If the provided
   * runner name is not registered via a {@link PipelineRunnerRegistrar}, we attempt to obtain the
   * class that the name represents using {@link Class#forName(String)} and use the result class if
   * it subclasses {@link PipelineRunner}.
   *
   * <p>If strict parsing is enabled, unknown options or options that cannot be converted to the
   * expected java type using an {@link ObjectMapper} will be ignored.
   */
  private static <T extends PipelineOptions> Map<String, Object> parseObjects(
      Class<T> klass, ListMultimap<String, String> options, boolean strictParsing) {
    Map<String, Method> propertyNamesToGetters = Maps.newHashMap();
    Cache cache = CACHE.get();
    cache.validateWellFormed(klass);
    @SuppressWarnings("unchecked")
    Iterable<PropertyDescriptor> propertyDescriptors =
        cache.getPropertyDescriptors(
            Stream.concat(getRegisteredOptions().stream(), Stream.of(klass))
                .collect(Collectors.toSet()));
    for (PropertyDescriptor descriptor : propertyDescriptors) {
      propertyNamesToGetters.put(descriptor.getName(), descriptor.getReadMethod());
    }
    Map<String, Object> convertedOptions = Maps.newHashMap();
    for (final Map.Entry<String, Collection<String>> entry : options.asMap().entrySet()) {
      try {
        // Search for close matches for missing properties.
        // Either off by one or off by two character errors.
        if (!propertyNamesToGetters.containsKey(entry.getKey())) {
          SortedSet<String> closestMatches =
              new TreeSet<>(
                  Sets.filter(
                      propertyNamesToGetters.keySet(),
                      input -> StringUtils.getLevenshteinDistance(entry.getKey(), input) <= 2));
          switch (closestMatches.size()) {
            case 0:
              throw new IllegalArgumentException(
                  String.format("Class %s missing a property named '%s'.", klass, entry.getKey()));
            case 1:
              throw new IllegalArgumentException(
                  String.format(
                      "Class %s missing a property named '%s'. Did you mean '%s'?",
                      klass, entry.getKey(), Iterables.getOnlyElement(closestMatches)));
            default:
              throw new IllegalArgumentException(
                  String.format(
                      "Class %s missing a property named '%s'. Did you mean one of %s?",
                      klass, entry.getKey(), closestMatches));
          }
        }
        Method method = propertyNamesToGetters.get(entry.getKey());
        // Only allow empty argument values for String, String Array, and Collection<String>.
        Class<?> returnType = method.getReturnType();
        JavaType type = MAPPER.getTypeFactory().constructType(method.getGenericReturnType());

        if ("runner".equals(entry.getKey())) {
          String runner = Iterables.getOnlyElement(entry.getValue());
          final Map<String, Class<? extends PipelineRunner<?>>> pipelineRunners =
              cache.supportedPipelineRunners;
          if (pipelineRunners.containsKey(runner.toLowerCase())) {
            convertedOptions.put("runner", pipelineRunners.get(runner.toLowerCase(ROOT)));
          } else {
            try {
              Class<?> runnerClass = Class.forName(runner, true, ReflectHelpers.findClassLoader());
              if (!PipelineRunner.class.isAssignableFrom(runnerClass)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Class '%s' does not implement PipelineRunner. "
                            + "Supported pipeline runners %s",
                        runner, cache.getSupportedRunners()));
              }
              convertedOptions.put("runner", runnerClass);
            } catch (ClassNotFoundException e) {
              String msg =
                  String.format(
                      "Unknown 'runner' specified '%s', supported pipeline runners %s",
                      runner, cache.getSupportedRunners());
              throw new IllegalArgumentException(msg, e);
            }
          }
        } else if (isCollectionOrArrayOfAllowedTypes(returnType, type)) {
          // Split any strings with ","
          List<String> values =
              entry.getValue().stream()
                  .flatMap(input -> Arrays.stream(input.split(",")))
                  .collect(Collectors.toList());

          if (values.contains("")) {
            checkEmptyStringAllowed(returnType, type, method.getGenericReturnType().toString());
          }
          convertedOptions.put(entry.getKey(), MAPPER.convertValue(values, type));
        } else if (isSimpleType(returnType, type)) {
          String value = Iterables.getOnlyElement(entry.getValue());
          if (value.isEmpty()) {
            checkEmptyStringAllowed(returnType, type, method.getGenericReturnType().toString());
          }
          convertedOptions.put(entry.getKey(), MAPPER.convertValue(value, type));
        } else {
          String value = Iterables.getOnlyElement(entry.getValue());
          if (value.isEmpty()) {
            checkEmptyStringAllowed(returnType, type, method.getGenericReturnType().toString());
          }
          try {
            convertedOptions.put(entry.getKey(), tryParseObject(value, method));
          } catch (IOException e) {
            throw new IllegalArgumentException("Unable to parse JSON value " + value, e);
          }
        }
      } catch (IllegalArgumentException e) {
        if (strictParsing) {
          throw e;
        } else {
          LOG.warn(
              "Strict parsing is disabled, ignoring option '{}' with value '{}' because {}",
              entry.getKey(),
              entry.getValue(),
              e.getMessage());
        }
      }
    }
    return convertedOptions;
  }

  /**
   * Returns true if the given type is one of {@code SIMPLE_TYPES} or an enum, or if the given type
   * is a {@link ValueProvider ValueProvider&lt;T&gt;} and {@code T} is one of {@code SIMPLE_TYPES}
   * or an enum.
   */
  private static boolean isSimpleType(Class<?> type, JavaType genericType) {
    Class<?> unwrappedType =
        type.equals(ValueProvider.class) ? genericType.containedType(0).getRawClass() : type;
    return SIMPLE_TYPES.contains(unwrappedType) || unwrappedType.isEnum();
  }

  /**
   * Returns true if the given type is an array or {@link Collection} of {@code SIMPLE_TYPES} or
   * enums, or if the given type is a {@link ValueProvider ValueProvider&lt;T&gt;} and {@code T} is
   * an array or {@link Collection} of {@code SIMPLE_TYPES} or enums.
   */
  private static boolean isCollectionOrArrayOfAllowedTypes(Class<?> type, JavaType genericType) {
    JavaType containerType =
        type.equals(ValueProvider.class) ? genericType.containedType(0) : genericType;

    // Check if it is an array of simple types or enum.
    if (containerType.getRawClass().isArray()
        && (SIMPLE_TYPES.contains(containerType.getRawClass().getComponentType())
            || containerType.getRawClass().getComponentType().isEnum())) {
      return true;
    }
    // Check if it is Collection of simple types or enum.
    if (Collection.class.isAssignableFrom(containerType.getRawClass())) {
      JavaType innerType = containerType.containedType(0);
      // Note that raw types are allowed, hence the null check.
      if (innerType == null
          || SIMPLE_TYPES.contains(innerType.getRawClass())
          || innerType.getRawClass().isEnum()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Ensures that empty string value is allowed for a given type.
   *
   * <p>Empty strings are only allowed for {@link String}, {@link String String[]}, {@link
   * Collection Collection&lt;String&gt;}, or {@link ValueProvider ValueProvider&lt;T&gt;} and
   * {@code T} is of type {@link String}, {@link String String[]}, {@link Collection
   * Collection&lt;String&gt;}.
   *
   * @param type class object for the type under check.
   * @param genericType complete type information for the type under check.
   * @param genericTypeName a string representation of the complete type information.
   */
  private static void checkEmptyStringAllowed(
      Class<?> type, JavaType genericType, String genericTypeName) {
    JavaType unwrappedType =
        type.equals(ValueProvider.class) ? genericType.containedType(0) : genericType;

    Class<?> containedType = unwrappedType.getRawClass();
    if (unwrappedType.getRawClass().isArray()) {
      containedType = unwrappedType.getRawClass().getComponentType();
    } else if (Collection.class.isAssignableFrom(unwrappedType.getRawClass())) {
      JavaType innerType = unwrappedType.containedType(0);
      // Note that raw types are allowed, hence the null check.
      containedType = innerType == null ? String.class : innerType.getRawClass();
    }
    if (!containedType.equals(String.class)) {
      String msg =
          String.format(
              "Empty argument value is only allowed for String, String Array, Collections of"
                  + " Strings or any of these types in a parameterized ValueProvider, but"
                  + " received: %s",
              genericTypeName);
      throw new IllegalArgumentException(msg);
    }
  }

  /** Hold all data which can change after a classloader change. */
  static final class Cache {
    private final Map<String, Class<? extends PipelineRunner<?>>> supportedPipelineRunners;

    /** The set of options that have been registered and visible to the user. */
    private final Set<Class<? extends PipelineOptions>> registeredOptions =
        Sets.newConcurrentHashSet();

    /** A cache storing a mapping from a given interface to its registration record. */
    private final Map<Class<? extends PipelineOptions>, Registration<?>> interfaceCache =
        Maps.newConcurrentMap();

    /** A cache storing a mapping from a set of interfaces to its registration record. */
    private final Map<Set<Class<? extends PipelineOptions>>, Registration<?>> combinedCache =
        Maps.newConcurrentMap();

    private final Map<Method, JsonDeserializer<Object>> deserializerCache = Maps.newConcurrentMap();

    private final Map<Method, Optional<JsonSerializer<Object>>> serializerCache =
        Maps.newConcurrentMap();

    private Cache() {
      final ClassLoader loader = ReflectHelpers.findClassLoader();
      // Store the list of all available pipeline runners.
      ImmutableMap.Builder<String, Class<? extends PipelineRunner<?>>> builder =
          ImmutableMap.builder();
      for (PipelineRunnerRegistrar registrar :
          ReflectHelpers.loadServicesOrdered(PipelineRunnerRegistrar.class, loader)) {
        for (Class<? extends PipelineRunner<?>> klass : registrar.getPipelineRunners()) {
          String runnerName = klass.getSimpleName().toLowerCase();
          builder.put(runnerName, klass);
          if (runnerName.endsWith("runner")) {
            builder.put(runnerName.substring(0, runnerName.length() - "Runner".length()), klass);
          }
        }
      }
      supportedPipelineRunners = builder.build();
      initializeRegistry(loader);
    }

    /** Load and register the list of all classes that extend PipelineOptions. */
    private void initializeRegistry(final ClassLoader loader) {
      for (PipelineOptionsRegistrar registrar :
          ReflectHelpers.loadServicesOrdered(PipelineOptionsRegistrar.class, loader)) {
        for (Class<? extends PipelineOptions> klass : registrar.getPipelineOptions()) {
          register(klass);
        }
      }
    }

    private synchronized void register(Class<? extends PipelineOptions> iface) {
      checkNotNull(iface);
      checkArgument(iface.isInterface(), "Only interface types are supported.");

      if (registeredOptions.contains(iface)) {
        return;
      }
      validateWellFormed(iface);
      registeredOptions.add(iface);
    }

    private <T extends PipelineOptions> Registration<T> validateWellFormed(Class<T> iface) {
      return validateWellFormed(iface, registeredOptions);
    }

    @VisibleForTesting
    Set<String> getSupportedRunners() {
      ImmutableSortedSet.Builder<String> supportedRunners = ImmutableSortedSet.naturalOrder();
      for (Class<? extends PipelineRunner<?>> runner : supportedPipelineRunners.values()) {
        supportedRunners.add(runner.getSimpleName());
      }
      return supportedRunners.build();
    }

    @VisibleForTesting
    Map<String, Class<? extends PipelineRunner<?>>> getSupportedPipelineRunners() {
      return supportedPipelineRunners;
    }

    /**
     * Validates that the interface conforms to the following:
     *
     * <ul>
     *   <li>Every inherited interface of {@code iface} must extend PipelineOptions except for
     *       PipelineOptions itself.
     *   <li>Any property with the same name must have the same return type for all derived
     *       interfaces of {@link PipelineOptions}.
     *   <li>Every bean property of any interface derived from {@link PipelineOptions} must have a
     *       getter and setter method.
     *   <li>Every method must conform to being a getter or setter for a JavaBean.
     *   <li>The derived interface of {@link PipelineOptions} must be composable with every
     *       interface part of allPipelineOptionsClasses.
     *   <li>Only getters may be annotated with {@link JsonIgnore @JsonIgnore}.
     *   <li>If any getter is annotated with {@link JsonIgnore @JsonIgnore}, then all getters for
     *       this property must be annotated with {@link JsonIgnore @JsonIgnore}.
     * </ul>
     *
     * @param iface The interface to validate.
     * @param validatedPipelineOptionsInterfaces The set of validated pipeline options interfaces to
     *     validate against.
     * @return A registration record containing the proxy class and bean info for iface.
     */
    synchronized <T extends PipelineOptions> Registration<T> validateWellFormed(
        Class<T> iface, Set<Class<? extends PipelineOptions>> validatedPipelineOptionsInterfaces) {
      checkArgument(iface.isInterface(), "Only interface types are supported.");

      // Validate that every inherited interface must extend PipelineOptions except for
      // PipelineOptions itself.
      validateInheritedInterfacesExtendPipelineOptions(iface);

      @SuppressWarnings("unchecked")
      Set<Class<? extends PipelineOptions>> combinedPipelineOptionsInterfaces =
          Stream.concat(validatedPipelineOptionsInterfaces.stream(), Stream.of(iface))
              .collect(Collectors.toSet());
      // Validate that the view of all currently passed in options classes is well formed.
      if (!combinedCache.containsKey(combinedPipelineOptionsInterfaces)) {
        final Class<?>[] interfaces = combinedPipelineOptionsInterfaces.toArray(EMPTY_CLASS_ARRAY);
        @SuppressWarnings("unchecked")
        Class<T> allProxyClass =
            (Class<T>) Proxy.getProxyClass(ReflectHelpers.findClassLoader(interfaces), interfaces);
        try {
          List<PropertyDescriptor> propertyDescriptors =
              validateClass(iface, validatedPipelineOptionsInterfaces, allProxyClass);
          combinedCache.put(
              combinedPipelineOptionsInterfaces,
              new Registration<>(allProxyClass, propertyDescriptors));
        } catch (IntrospectionException e) {
          throw new RuntimeException(e);
        }
      }

      // Validate that the local view of the class is well formed.
      if (!interfaceCache.containsKey(iface)) {
        @SuppressWarnings({
          "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
          "unchecked"
        })
        Class<T> proxyClass =
            (Class<T>)
                Proxy.getProxyClass(ReflectHelpers.findClassLoader(iface), new Class[] {iface});
        try {
          List<PropertyDescriptor> propertyDescriptors =
              validateClass(iface, validatedPipelineOptionsInterfaces, proxyClass);
          interfaceCache.put(iface, new Registration<>(proxyClass, propertyDescriptors));
        } catch (IntrospectionException e) {
          throw new RuntimeException(e);
        }
      }
      @SuppressWarnings("unchecked")
      Registration<T> result = (Registration<T>) interfaceCache.get(iface);
      return result;
    }

    List<PropertyDescriptor> getPropertyDescriptors(
        Set<Class<? extends PipelineOptions>> interfaces) {
      return combinedCache.get(interfaces).getPropertyDescriptors();
    }
  }
}
