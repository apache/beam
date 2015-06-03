/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunnerRegistrar;
import com.google.cloud.dataflow.sdk.runners.worker.DataflowWorkerHarness;
import com.google.cloud.dataflow.sdk.util.common.ReflectHelpers;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Constructs a {@link PipelineOptions} or any derived interface that is composable to any other
 * derived interface of {@link PipelineOptions} via the {@link PipelineOptions#as} method. Being
 * able to compose one derived interface of {@link PipelineOptions} to another has the following
 * restrictions:
 * <ul>
 *   <li>Any property with the same name must have the same return type for all derived interfaces
 *       of {@link PipelineOptions}.
 *   <li>Every bean property of any interface derived from {@link PipelineOptions} must have a
 *       getter and setter method.
 *   <li>Every method must conform to being a getter or setter for a JavaBean.
 *   <li>The derived interface of {@link PipelineOptions} must be composable with every interface
 *       registered with this factory.
 * </ul>
 * <p>
 * See the <a
 * href="http://www.oracle.com/technetwork/java/javase/documentation/spec-136004.html">JavaBeans
 * specification</a> for more details as to what constitutes a property.
 */
public class PipelineOptionsFactory {

  /**
   * Creates and returns an object that implements {@link PipelineOptions}.
   * This sets the {@link ApplicationNameOptions#getAppName() "appName"} to the calling
   * {@link Class#getSimpleName() classes simple name}.
   *
   * @return An object that implements {@link PipelineOptions}.
   */
  public static PipelineOptions create() {
    return new Builder().as(PipelineOptions.class);
  }

  /**
   * Creates and returns an object that implements {@code <T>}.
   * This sets the {@link ApplicationNameOptions#getAppName() "appName"} to the calling
   * {@link Class#getSimpleName() classes simple name}.
   *
   * <p> Note that {@code <T>} must be composable with every registered interface with this factory.
   * See {@link PipelineOptionsFactory#validateWellFormed(Class, Set)} for more details.
   *
   * @return An object that implements {@code <T>}.
   */
  public static <T extends PipelineOptions> T as(Class<T> klass) {
    return new Builder().as(klass);
  }

  /**
   * Sets the command line arguments to parse when constructing the {@link PipelineOptions}.
   * <p>
   * Example GNU style command line arguments:
   * <pre>
   *   --project=MyProject (simple property, will set the "project" property to "MyProject")
   *   --readOnly=true (for boolean properties, will set the "readOnly" property to "true")
   *   --readOnly (shorthand for boolean properties, will set the "readOnly" property to "true")
   *   --x=1 --x=2 --x=3 (list style property, will set the "x" property to [1, 2, 3])
   *   --x=1,2,3 (shorthand list style property, will set the "x" property to [1, 2, 3])
   * </pre>
   * Properties are able to bound to {@link String} and Java primitives {@code boolean},
   * {@code byte}, {@code short}, {@code int}, {@code long}, {@code float}, {@code double} and
   * their primitive wrapper classes.
   * <p>
   * List style properties are able to be bound to {@code boolean[]}, {@code char[]},
   * {@code short[]}, {@code int[]}, {@code long[]}, {@code float[]}, {@code double[]},
   * {@code String[]} and {@code List<String>}.
   * <p>
   * By default, strict parsing is enabled and arguments must conform to be either
   * {@code --booleanArgName} or {@code --argName=argValue}. Strict parsing can be disabled with
   * {@link Builder#withoutStrictParsing()}. Empty or null arguments will be ignored whether
   * or not strict parsing is enabled.
   * <p>
   * Help information can be output to {@link System#out} by specifying {@code --help} as an
   * argument. After help is printed, the application will exit. Specifying only {@code --help}
   * will print out the list of
   * {@link PipelineOptionsFactory#getRegisteredOptions() registered options}
   * by invoking {@link PipelineOptionsFactory#printHelp(PrintStream)}. Specifying
   * {@code --help=PipelineOptionsClassName} will print out detailed usage information about the
   * specifically requested PipelineOptions by invoking
   * {@link PipelineOptionsFactory#printHelp(PrintStream, Class)}.
   */
  public static Builder fromArgs(String[] args) {
    return new Builder().fromArgs(args);
  }

  /**
   * After creation we will validate that {@link PipelineOptions} conforms to all the
   * validation criteria from {@code <T>}. See
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

    // Do not allow direct instantiation
    private Builder() {
      this(null, false, true);
    }

    private Builder(String[] args, boolean validation,
        boolean strictParsing) {
      this.defaultAppName = findCallersClassName();
      this.args = args;
      this.validation = validation;
      this.strictParsing = strictParsing;
    }

    /**
     * Sets the command line arguments to parse when constructing the {@link PipelineOptions}.
     * <p>
     * Example GNU style command line arguments:
     * <pre>
     *   --project=MyProject (simple property, will set the "project" property to "MyProject")
     *   --readOnly=true (for boolean properties, will set the "readOnly" property to "true")
     *   --readOnly (shorthand for boolean properties, will set the "readOnly" property to "true")
     *   --x=1 --x=2 --x=3 (list style property, will set the "x" property to [1, 2, 3])
     *   --x=1,2,3 (shorthand list style property, will set the "x" property to [1, 2, 3])
     * </pre>
     * Properties are able to bound to {@link String} and Java primitives {@code boolean},
     * {@code byte}, {@code short}, {@code int}, {@code long}, {@code float}, {@code double} and
     * their primitive wrapper classes.
     * <p>
     * List style properties are able to be bound to {@code boolean[]}, {@code char[]},
     * {@code short[]}, {@code int[]}, {@code long[]}, {@code float[]}, {@code double[]},
     * {@code String[]} and {@code List<String>}.
     * <p>
     * By default, strict parsing is enabled and arguments must conform to be either
     * {@code --booleanArgName} or {@code --argName=argValue}. Strict parsing can be disabled with
     * {@link Builder#withoutStrictParsing()}. Empty or null arguments will be ignored whether
     * or not strict parsing is enabled.
     * <p>
     * Help information can be output to {@link System#out} by specifying {@code --help} as an
     * argument. After help is printed, the application will exit. Specifying only {@code --help}
     * will print out the list of
     * {@link PipelineOptionsFactory#getRegisteredOptions() registered options}
     * by invoking {@link PipelineOptionsFactory#printHelp(PrintStream)}. Specifying
     * {@code --help=PipelineOptionsClassName} will print out detailed usage information about the
     * specifically requested PipelineOptions by invoking
     * {@link PipelineOptionsFactory#printHelp(PrintStream, Class)}.
     */
    public Builder fromArgs(String[] args) {
      Preconditions.checkNotNull(args, "Arguments should not be null.");
      return new Builder(args, validation, strictParsing);
    }

    /**
     * After creation we will validate that {@link PipelineOptions} conforms to all the
     * validation criteria from {@code <T>}. See
     * {@link PipelineOptionsValidator#validate(Class, PipelineOptions)} for more details about
     * validation.
     */
    public Builder withValidation() {
      return new Builder(args, true, strictParsing);
    }

    /**
     * During parsing of the arguments, we will skip over improperly formatted and unknown
     * arguments.
     */
    public Builder withoutStrictParsing() {
      return new Builder(args, validation, false);
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
     * Creates and returns an object that implements {@code <T>} using the values configured on
     * this builder during construction.
     * <p>
     * Note that {@code <T>} must be composable with every registered interface with this factory.
     * See {@link PipelineOptionsFactory#validateWellFormed(Class, Set)} for more details.
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

      if (validation) {
        PipelineOptionsValidator.validate(klass, t);
      }
      return t;
    }
  }

  /**
   * Determines whether the generic {@code --help} was requested or help was
   * requested for a specific class and invokes the appropriate
   * {@link PipelineOptionsFactory#printHelp(PrintStream)} and
   * {@link PipelineOptionsFactory#printHelp(PrintStream, Class)} variant.
   * Prints to the specified {@link PrintStream}, and exits if requested.
   * <p>
   * Visible for testing.
   * {@code printStream} and {@code exit} used for testing.
   */
  @SuppressWarnings("unchecked")
  static boolean printHelpUsageAndExitIfNeeded(ListMultimap<String, String> options,
      PrintStream printStream, boolean exit) {
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
        Class<?> klass = Class.forName(helpOption);
        if (!PipelineOptions.class.isAssignableFrom(klass)) {
          throw new ClassNotFoundException("PipelineOptions of type " + klass + " not found.");
        }
        printHelp(printStream, (Class<? extends PipelineOptions>) klass);
      } catch (ClassNotFoundException e) {
        // If we didn't find an exact match, look for any that match the class name.
        Iterable<Class<? extends PipelineOptions>> matches = Iterables.filter(
            getRegisteredOptions(),
            new Predicate<Class<? extends PipelineOptions>>() {
              @Override
              public boolean apply(Class<? extends PipelineOptions> input) {
                if (helpOption.contains(".")) {
                  return input.getName().endsWith(helpOption);
                } else {
                  return input.getSimpleName().equals(helpOption);
                }
              }
          });
        try {
          printHelp(printStream, Iterables.getOnlyElement(matches));
        } catch (NoSuchElementException exception) {
          printStream.format("Unable to find option %s.%n", helpOption);
          printHelp(printStream);
        } catch (IllegalArgumentException exception) {
          printStream.format("Multiple matches found for %s: %s.%n", helpOption,
              Iterables.transform(matches, ReflectHelpers.CLASS_NAME));
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

  /**
   * Returns the simple name of the calling class using the current threads stack.
   */
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
          return Class.forName(next.getClassName()).getSimpleName();
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


  private static final Logger LOG = LoggerFactory.getLogger(PipelineOptionsFactory.class);
  @SuppressWarnings("rawtypes")
  private static final Class<?>[] EMPTY_CLASS_ARRAY = new Class[0];
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Map<String, Class<? extends PipelineRunner<?>>> SUPPORTED_PIPELINE_RUNNERS;

  /** Classes that are used as the boundary in the stack trace to find the callers class name. */
  private static final Set<String> PIPELINE_OPTIONS_FACTORY_CLASSES = ImmutableSet.of(
      PipelineOptionsFactory.class.getName(),
      Builder.class.getName());

  /** Methods that are ignored when validating the proxy class. */
  private static final Set<Method> IGNORED_METHODS;

  /** The set of options that have been registered and visible to the user. */
  private static final Set<Class<? extends PipelineOptions>> REGISTERED_OPTIONS =
      Sets.newConcurrentHashSet();

  /** A cache storing a mapping from a given interface to its registration record. */
  private static final Map<Class<? extends PipelineOptions>, Registration<?>> INTERFACE_CACHE =
      Maps.newConcurrentMap();

  /** A cache storing a mapping from a set of interfaces to its registration record. */
  private static final Map<Set<Class<? extends PipelineOptions>>, Registration<?>> COMBINED_CACHE =
      Maps.newConcurrentMap();

  /** The width at which options should be output. */
  private static final int TERMINAL_WIDTH = 80;

  /**
   * Finds the appropriate {@code ClassLoader} to be used by the
   * {@link ServiceLoader#load} call, which by default would use the context
   * {@code ClassLoader}, which can be null. The fallback is as follow: context
   * ClassLoader, class ClassLoader and finaly the system ClassLoader.
   */
  static ClassLoader findClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = PipelineOptionsFactory.class.getClassLoader();
    }
    if (classLoader == null) {
      classLoader = ClassLoader.getSystemClassLoader();
    }
    return classLoader;
  }

  static {
    try {
      IGNORED_METHODS = ImmutableSet.<Method>builder()
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

    ClassLoader classLoader = findClassLoader();

    // Store the list of all available pipeline runners.
    ImmutableMap.Builder<String, Class<? extends PipelineRunner<?>>> builder =
            ImmutableMap.builder();
    Set<PipelineRunnerRegistrar> pipelineRunnerRegistrars =
        Sets.newTreeSet(ObjectsClassComparator.INSTANCE);
    pipelineRunnerRegistrars.addAll(
        Lists.newArrayList(ServiceLoader.load(PipelineRunnerRegistrar.class, classLoader)));
    for (PipelineRunnerRegistrar registrar : pipelineRunnerRegistrars) {
      for (Class<? extends PipelineRunner<?>> klass : registrar.getPipelineRunners()) {
        builder.put(klass.getSimpleName(), klass);
      }
    }
    SUPPORTED_PIPELINE_RUNNERS = builder.build();

    // Load and register the list of all classes that extend PipelineOptions.
    register(PipelineOptions.class);
    Set<PipelineOptionsRegistrar> pipelineOptionsRegistrars =
        Sets.newTreeSet(ObjectsClassComparator.INSTANCE);
    pipelineOptionsRegistrars.addAll(
        Lists.newArrayList(ServiceLoader.load(PipelineOptionsRegistrar.class, classLoader)));
    for (PipelineOptionsRegistrar registrar : pipelineOptionsRegistrars) {
      for (Class<? extends PipelineOptions> klass : registrar.getPipelineOptions()) {
        register(klass);
      }
    }
  }

  /**
   * This registers the interface with this factory. This interface must conform to the following
   * restrictions:
   * <ul>
   *   <li>Any property with the same name must have the same return type for all derived
   *       interfaces of {@link PipelineOptions}.
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
    Preconditions.checkNotNull(iface);
    Preconditions.checkArgument(iface.isInterface(), "Only interface types are supported.");

    if (REGISTERED_OPTIONS.contains(iface)) {
      return;
    }
    validateWellFormed(iface, REGISTERED_OPTIONS);
    REGISTERED_OPTIONS.add(iface);
  }

  /**
   * Validates that the interface conforms to the following:
   * <ul>
   *   <li>Any property with the same name must have the same return type for all derived
   *       interfaces of {@link PipelineOptions}.
   *   <li>Every bean property of any interface derived from {@link PipelineOptions} must have a
   *       getter and setter method.
   *   <li>Every method must conform to being a getter or setter for a JavaBean.
   *   <li>The derived interface of {@link PipelineOptions} must be composable with every interface
   *       part of allPipelineOptionsClasses.
   *   <li>Only getters may be annotated with {@link JsonIgnore @JsonIgnore}.
   *   <li>If any getter is annotated with {@link JsonIgnore @JsonIgnore}, then all getters for
   *       this property must be annotated with {@link JsonIgnore @JsonIgnore}.
   * </ul>
   *
   * @param iface The interface to validate.
   * @param validatedPipelineOptionsInterfaces The set of validated pipeline options interfaces to
   *        validate against.
   * @return A registration record containing the proxy class and bean info for iface.
   */
  static synchronized <T extends PipelineOptions> Registration<T> validateWellFormed(
      Class<T> iface, Set<Class<? extends PipelineOptions>> validatedPipelineOptionsInterfaces) {
    Preconditions.checkArgument(iface.isInterface(), "Only interface types are supported.");

    @SuppressWarnings("unchecked")
    Set<Class<? extends PipelineOptions>> combinedPipelineOptionsInterfaces =
        FluentIterable.from(validatedPipelineOptionsInterfaces).append(iface).toSet();
    // Validate that the view of all currently passed in options classes is well formed.
    if (!COMBINED_CACHE.containsKey(combinedPipelineOptionsInterfaces)) {
      @SuppressWarnings("unchecked")
      Class<T> allProxyClass =
          (Class<T>) Proxy.getProxyClass(PipelineOptionsFactory.class.getClassLoader(),
              combinedPipelineOptionsInterfaces.toArray(EMPTY_CLASS_ARRAY));
      try {
        List<PropertyDescriptor> propertyDescriptors =
            validateClass(iface, validatedPipelineOptionsInterfaces, allProxyClass);
        COMBINED_CACHE.put(combinedPipelineOptionsInterfaces,
            new Registration<T>(allProxyClass, propertyDescriptors));
      } catch (IntrospectionException e) {
        throw Throwables.propagate(e);
      }
    }

    // Validate that the local view of the class is well formed.
    if (!INTERFACE_CACHE.containsKey(iface)) {
      @SuppressWarnings({"rawtypes", "unchecked"})
      Class<T> proxyClass = (Class<T>) Proxy.getProxyClass(
          PipelineOptionsFactory.class.getClassLoader(), new Class[] {iface});
      try {
        List<PropertyDescriptor> propertyDescriptors =
            validateClass(iface, validatedPipelineOptionsInterfaces, proxyClass);
        INTERFACE_CACHE.put(iface,
            new Registration<T>(proxyClass, propertyDescriptors));
      } catch (IntrospectionException e) {
        throw Throwables.propagate(e);
      }
    }
    @SuppressWarnings("unchecked")
    Registration<T> result = (Registration<T>) INTERFACE_CACHE.get(iface);
    return result;
  }

  public static Set<Class<? extends PipelineOptions>> getRegisteredOptions() {
    return Collections.unmodifiableSet(REGISTERED_OPTIONS);
  }

  /**
   * Outputs the set of registered options with the PipelineOptionsFactory
   * with a description for each one if available to the output stream. This output
   * is pretty printed and meant to be human readable. This method will attempt to
   * format its output to be compatible with a terminal window.
   */
  public static void printHelp(PrintStream out) {
    Preconditions.checkNotNull(out);
    out.println("The set of registered options are:");
    Set<Class<? extends PipelineOptions>> sortedOptions =
        new TreeSet<>(ClassNameComparator.INSTANCE);
    sortedOptions.addAll(REGISTERED_OPTIONS);
    for (Class<? extends PipelineOptions> kls : sortedOptions) {
      out.format("  %s%n", kls.getName());
    }
    out.format("%nUse --help=<OptionsName> for detailed help. For example:%n"
        + "  --help=DataflowPipelineOptions <short names valid for registered options>%n"
        + "  --help=com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions%n");
  }

  /**
   * Outputs the set of options available to be set for the passed in {@link PipelineOptions}
   * interface. The output is in a human readable format. The format is:
   * <pre>
   * OptionGroup:
   *     ... option group description ...
   *
   *  --option1={@code <type>} or list of valid enum choices
   *     Default: value (if available, see {@link Default})
   *     ... option description ... (if available, see {@link Description})
   *  --option2={@code <type>} or list of valid enum choices
   *     Default: value (if available, see {@link Default})
   *     ... option description ... (if available, see {@link Description})
   * </pre>
   * This method will attempt to format its output to be compatible with a terminal window.
   */
  public static void printHelp(PrintStream out, Class<? extends PipelineOptions> iface) {
    Preconditions.checkNotNull(out);
    Preconditions.checkNotNull(iface);
    validateWellFormed(iface, REGISTERED_OPTIONS);

    Iterable<Method> methods = ReflectHelpers.getClosureOfMethodsOnInterface(iface);
    ListMultimap<Class<?>, Method> ifaceToMethods = ArrayListMultimap.create();
    for (Method method : methods) {
      // Process only methods that are not marked as hidden.
      if (method.getAnnotation(Hidden.class) == null) {
        ifaceToMethods.put(method.getDeclaringClass(), method);
      }
    }
    SortedSet<Class<?>> ifaces = new TreeSet<>(ClassNameComparator.INSTANCE);
    // Keep interfaces that are not marked as hidden.
    ifaces.addAll(Collections2.filter(ifaceToMethods.keySet(), new Predicate<Class<?>>() {
      @Override
      public boolean apply(Class<?> input) {
        return input.getAnnotation(Hidden.class) == null;
      }
    }));
    for (Class<?> currentIface : ifaces) {
      Map<String, Method> propertyNamesToGetters =
          getPropertyNamesToGetters(ifaceToMethods.get(currentIface));

      // Don't output anything if there are no defined options
      if (propertyNamesToGetters.isEmpty()) {
        continue;
      }

      out.format("%s:%n", currentIface.getName());
      prettyPrintDescription(out, currentIface.getAnnotation(Description.class));

      out.println();

      List<String> lists = Lists.newArrayList(propertyNamesToGetters.keySet());
      Collections.sort(lists, String.CASE_INSENSITIVE_ORDER);
      for (String propertyName : lists) {
        Method method = propertyNamesToGetters.get(propertyName);
        String printableType = method.getReturnType().getSimpleName();
        if (method.getReturnType().isEnum()) {
          printableType = Joiner.on(" | ").join(method.getReturnType().getEnumConstants());
        }
        out.format("  --%s=<%s>%n", propertyName, printableType);
        Optional<String> defaultValue = getDefaultValueFromAnnotation(method);
        if (defaultValue.isPresent()) {
          out.format("    Default: %s%n", defaultValue.get());
        }
        prettyPrintDescription(out, method.getAnnotation(Description.class));
      }
      out.println();
    }
  }

  /**
   * Outputs the value of the description, breaking up long lines on white space characters
   * and attempting to honor a line limit of {@code TERMINAL_WIDTH}.
   */
  private static void prettyPrintDescription(PrintStream out, Description description) {
    final String spacing = "   ";
    if (description == null || description.value() == null) {
      return;
    }

    String[] words = description.value().split("\\s+");
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

  /**
   * Returns a string representation of the {@link Default} value on the passed in method.
   */
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
    return SUPPORTED_PIPELINE_RUNNERS;
  }

  static List<PropertyDescriptor> getPropertyDescriptors(
      Set<Class<? extends PipelineOptions>> interfaces) {
    return COMBINED_CACHE.get(interfaces).getPropertyDescriptors();
  }

  /**
   * Creates a set of {@link DataflowWorkerHarnessOptions} based of a set of known system
   * properties. This is meant to only be used from the {@link DataflowWorkerHarness} as a method to
   * bootstrap the worker harness.
   *
   * <p>For internal use only.
   *
   * @return A {@link DataflowWorkerHarnessOptions} object configured for the
   *         {@link DataflowWorkerHarness}.
   */
  public static DataflowWorkerHarnessOptions createFromSystemPropertiesInternal()
      throws IOException {
    return createFromSystemProperties();
  }

  /**
   * Creates a set of {@link DataflowWorkerHarnessOptions} based of a set of known system
   * properties. This is meant to only be used from the {@link DataflowWorkerHarness} as a method to
   * bootstrap the worker harness.
   *
   * @return A {@link DataflowWorkerHarnessOptions} object configured for the
   *         {@link DataflowWorkerHarness}.
   * @deprecated for internal use only
   */
  @Deprecated
  public static DataflowWorkerHarnessOptions createFromSystemProperties() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    DataflowWorkerHarnessOptions options;
    if (System.getProperties().containsKey("sdk_pipeline_options")) {
      String serializedOptions = System.getProperty("sdk_pipeline_options");
      LOG.info("Worker harness starting with: " + serializedOptions);
      options = objectMapper.readValue(serializedOptions, PipelineOptions.class)
          .as(DataflowWorkerHarnessOptions.class);
    } else {
      options = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    }

    // These values will not be known at job submission time and must be provided.
    if (System.getProperties().containsKey("worker_id")) {
      options.setWorkerId(System.getProperty("worker_id"));
    }
    if (System.getProperties().containsKey("job_id")) {
      options.setJobId(System.getProperty("job_id"));
    }

    return options;
  }

  /**
   * This method is meant to emulate the behavior of {@link Introspector#getBeanInfo(Class, int)}
   * to construct the list of {@link PropertyDescriptor}.
   * <p>
   * TODO: Swap back to using Introspector once the proxy class issue with AppEngine is resolved.
   */
  private static List<PropertyDescriptor> getPropertyDescriptors(Class<?> beanClass)
      throws IntrospectionException {
    // The sorting is important to make this method stable.
    SortedSet<Method> methods = Sets.newTreeSet(MethodComparator.INSTANCE);
    methods.addAll(Arrays.asList(beanClass.getMethods()));
    SortedMap<String, Method> propertyNamesToGetters = getPropertyNamesToGetters(methods);
    List<PropertyDescriptor> descriptors = Lists.newArrayList();

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
        Class<?> getterPropertyType = getterMethod.getReturnType();
        Class<?> setterPropertyType = method.getParameterTypes()[0];
        Preconditions.checkArgument(getterPropertyType == setterPropertyType,
            "Type mismatch between getter and setter methods for property [%s]. "
            + "Getter is of type [%s] whereas setter is of type [%s].",
            propertyName, getterPropertyType.getName(), setterPropertyType.getName());
      }

      descriptors.add(new PropertyDescriptor(
          propertyName, getterMethod, method));
    }

    // Add the remaining getters with missing setters.
    for (Map.Entry<String, Method> getterToMethod : propertyNamesToGetters.entrySet()) {
      descriptors.add(new PropertyDescriptor(
          getterToMethod.getKey(), getterToMethod.getValue(), null));
    }
    return descriptors;
  }

  /**
   * Returns a map of the property name to the getter method it represents.
   * If there are duplicate methods with the same bean name, then it is indeterminate
   * as to which method will be returned.
   */
  private static SortedMap<String, Method> getPropertyNamesToGetters(Iterable<Method> methods) {
    SortedMap<String, Method> propertyNamesToGetters = Maps.newTreeMap();
    for (Method method : methods) {
      String methodName = method.getName();
      if ((!methodName.startsWith("get")
          && !methodName.startsWith("is"))
          || method.getParameterTypes().length != 0
          || method.getReturnType() == void.class) {
        continue;
      }
      String propertyName = Introspector.decapitalize(
          methodName.startsWith("is") ? methodName.substring(2) : methodName.substring(3));
      propertyNamesToGetters.put(propertyName, method);
    }
    return propertyNamesToGetters;
  }

  /**
   * Validates that a given class conforms to the following properties:
   * <ul>
   *   <li>Any property with the same name must have the same return type for all derived
   *       interfaces of {@link PipelineOptions}.
   *   <li>Every bean property of any interface derived from {@link PipelineOptions} must have a
   *       getter and setter method.
   *   <li>Every method must conform to being a getter or setter for a JavaBean.
   *   <li>Only getters may be annotated with {@link JsonIgnore @JsonIgnore}.
   *   <li>If any getter is annotated with {@link JsonIgnore @JsonIgnore}, then all getters for
   *       this property must be annotated with {@link JsonIgnore @JsonIgnore}.
   * </ul>
   *
   * @param iface The interface to validate.
   * @param validatedPipelineOptionsInterfaces The set of validated pipeline options interfaces to
   *        validate against.
   * @param klass The proxy class representing the interface.
   * @return A list of {@link PropertyDescriptor}s representing all valid bean properties of
   *         {@code iface}.
   * @throws IntrospectionException if invalid property descriptors.
   */
  private static List<PropertyDescriptor> validateClass(Class<? extends PipelineOptions> iface,
      Set<Class<? extends PipelineOptions>> validatedPipelineOptionsInterfaces,
      Class<?> klass) throws IntrospectionException {
    Set<Method> methods = Sets.newHashSet(IGNORED_METHODS);
    // Ignore static methods, "equals", "hashCode", "toString" and "as" on the generated class.
    for (Method method : klass.getMethods()) {
      if (Modifier.isStatic(method.getModifiers())) {
        methods.add(method);
      }
    }
    try {
      methods.add(klass.getMethod("equals", Object.class));
      methods.add(klass.getMethod("hashCode"));
      methods.add(klass.getMethod("toString"));
      methods.add(klass.getMethod("as", Class.class));
      methods.add(klass.getMethod("cloneAs", Class.class));
    } catch (NoSuchMethodException | SecurityException e) {
      throw Throwables.propagate(e);
    }

    // Verify that there are no methods with the same name with two different return types.
    Iterable<Method> interfaceMethods = FluentIterable
        .from(ReflectHelpers.getClosureOfMethodsOnInterface(iface))
        .toSortedSet(MethodComparator.INSTANCE);
    SortedSetMultimap<Method, Method> methodNameToMethodMap =
        TreeMultimap.create(MethodNameComparator.INSTANCE, MethodComparator.INSTANCE);
    for (Method method : interfaceMethods) {
      methodNameToMethodMap.put(method, method);
    }
    for (Map.Entry<Method, Collection<Method>> entry
        : methodNameToMethodMap.asMap().entrySet()) {
      Set<Class<?>> returnTypes = FluentIterable.from(entry.getValue())
          .transform(ReturnTypeFetchingFunction.INSTANCE).toSet();
      SortedSet<Method> collidingMethods = FluentIterable.from(entry.getValue())
          .toSortedSet(MethodComparator.INSTANCE);
      Preconditions.checkArgument(returnTypes.size() == 1,
          "Method [%s] has multiple definitions %s with different return types for [%s].",
          entry.getKey().getName(),
          collidingMethods,
          iface.getName());
    }

    // Verify that there is no getter with a mixed @JsonIgnore annotation and verify
    // that no setter has @JsonIgnore.
    Iterable<Method> allInterfaceMethods = FluentIterable
        .from(ReflectHelpers.getClosureOfMethodsOnInterfaces(validatedPipelineOptionsInterfaces))
        .append(ReflectHelpers.getClosureOfMethodsOnInterface(iface))
        .toSortedSet(MethodComparator.INSTANCE);
    SortedSetMultimap<Method, Method> methodNameToAllMethodMap =
        TreeMultimap.create(MethodNameComparator.INSTANCE, MethodComparator.INSTANCE);
    for (Method method : allInterfaceMethods) {
      methodNameToAllMethodMap.put(method, method);
    }

    List<PropertyDescriptor> descriptors = getPropertyDescriptors(klass);

    for (PropertyDescriptor descriptor : descriptors) {
      if (descriptor.getReadMethod() == null
          || descriptor.getWriteMethod() == null
          || IGNORED_METHODS.contains(descriptor.getReadMethod())
          || IGNORED_METHODS.contains(descriptor.getWriteMethod())) {
        continue;
      }
      SortedSet<Method> getters = methodNameToAllMethodMap.get(descriptor.getReadMethod());
      SortedSet<Method> gettersWithJsonIgnore = Sets.filter(getters, JsonIgnorePredicate.INSTANCE);

      Iterable<String> getterClassNames = FluentIterable.from(getters)
          .transform(MethodToDeclaringClassFunction.INSTANCE)
          .transform(ReflectHelpers.CLASS_NAME);
      Iterable<String> gettersWithJsonIgnoreClassNames = FluentIterable.from(gettersWithJsonIgnore)
          .transform(MethodToDeclaringClassFunction.INSTANCE)
          .transform(ReflectHelpers.CLASS_NAME);

      Preconditions.checkArgument(gettersWithJsonIgnore.isEmpty()
          || getters.size() == gettersWithJsonIgnore.size(),
          "Expected getter for property [%s] to be marked with @JsonIgnore on all %s, "
          + "found only on %s",
          descriptor.getName(), getterClassNames, gettersWithJsonIgnoreClassNames);

      SortedSet<Method> settersWithJsonIgnore =
          Sets.filter(methodNameToAllMethodMap.get(descriptor.getWriteMethod()),
              JsonIgnorePredicate.INSTANCE);

      Iterable<String> settersWithJsonIgnoreClassNames = FluentIterable.from(settersWithJsonIgnore)
          .transform(MethodToDeclaringClassFunction.INSTANCE)
          .transform(ReflectHelpers.CLASS_NAME);

      Preconditions.checkArgument(settersWithJsonIgnore.isEmpty(),
          "Expected setter for property [%s] to not be marked with @JsonIgnore on %s",
          descriptor.getName(), settersWithJsonIgnoreClassNames);
    }

    // Verify that each property has a matching read and write method.
    for (PropertyDescriptor propertyDescriptor : descriptors) {
      Preconditions.checkArgument(
          IGNORED_METHODS.contains(propertyDescriptor.getWriteMethod())
          || propertyDescriptor.getReadMethod() != null,
          "Expected getter for property [%s] of type [%s] on [%s].",
          propertyDescriptor.getName(),
          propertyDescriptor.getPropertyType().getName(),
          iface.getName());
      Preconditions.checkArgument(
          IGNORED_METHODS.contains(propertyDescriptor.getReadMethod())
          || propertyDescriptor.getWriteMethod() != null,
          "Expected setter for property [%s] of type [%s] on [%s].",
          propertyDescriptor.getName(),
          propertyDescriptor.getPropertyType().getName(),
          iface.getName());
      methods.add(propertyDescriptor.getReadMethod());
      methods.add(propertyDescriptor.getWriteMethod());
    }

    // Verify that no additional methods are on an interface that aren't a bean property.
    SortedSet<Method> unknownMethods = new TreeSet<>(MethodComparator.INSTANCE);
    unknownMethods.addAll(Sets.difference(Sets.newHashSet(klass.getMethods()), methods));
    Preconditions.checkArgument(unknownMethods.isEmpty(),
        "Methods %s on [%s] do not conform to being bean properties.",
        FluentIterable.from(unknownMethods).transform(ReflectHelpers.METHOD_FORMATTER),
        iface.getName());

    return descriptors;
  }

  /** A {@link Comparator} that uses the classes name to compare them. */
  private static class ClassNameComparator implements Comparator<Class<?>> {
    static final ClassNameComparator INSTANCE = new ClassNameComparator();
    @Override
    public int compare(Class<?> o1, Class<?> o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }

  /** A {@link Comparator} that uses the object's classes canonical name to compare them. */
  private static class ObjectsClassComparator implements Comparator<Object> {
    static final ObjectsClassComparator INSTANCE = new ObjectsClassComparator();
    @Override
    public int compare(Object o1, Object o2) {
      return o1.getClass().getCanonicalName().compareTo(o2.getClass().getCanonicalName());
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
  private static class MethodNameComparator implements Comparator<Method> {
    static final MethodNameComparator INSTANCE = new MethodNameComparator();
    @Override
    public int compare(Method o1, Method o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }

  /** A {@link Function} that gets the method's return type. */
  private static class ReturnTypeFetchingFunction implements Function<Method, Class<?>> {
    static final ReturnTypeFetchingFunction INSTANCE = new ReturnTypeFetchingFunction();
    @Override
    public Class<?> apply(Method input) {
      return input.getReturnType();
    }
  }

  /** A {@link Function} with returns the declaring class for the method. */
  private static class MethodToDeclaringClassFunction implements Function<Method, Class<?>> {
    static final MethodToDeclaringClassFunction INSTANCE = new MethodToDeclaringClassFunction();
    @Override
    public Class<?> apply(Method input) {
      return input.getDeclaringClass();
    }
  }

  /**
   * A {@link Predicate} that returns true if the method is annotated with
   * {@link JsonIgnore @JsonIgnore}.
   */
  static class JsonIgnorePredicate implements Predicate<Method> {
    static final JsonIgnorePredicate INSTANCE = new JsonIgnorePredicate();
    @Override
    public boolean apply(Method input) {
      return input.isAnnotationPresent(JsonIgnore.class);
    }
  }

  /**
   * Splits string arguments based upon expected pattern of --argName=value.
   *
   * <p> Example GNU style command line arguments:
   *
   * <pre>
   *   --project=MyProject (simple property, will set the "project" property to "MyProject")
   *   --readOnly=true (for boolean properties, will set the "readOnly" property to "true")
   *   --readOnly (shorthand for boolean properties, will set the "readOnly" property to "true")
   *   --x=1 --x=2 --x=3 (list style property, will set the "x" property to [1, 2, 3])
   *   --x=1,2,3 (shorthand list style property, will set the "x" property to [1, 2, 3])
   * </pre>
   *
   * <p> Properties are able to bound to {@link String} and Java primitives {@code boolean},
   * {@code byte}, {@code short}, {@code int}, {@code long}, {@code float}, {@code double}
   * and their primitive wrapper classes.
   *
   * <p> List style properties are able to be bound to {@code boolean[]}, {@code char[]},
   * {@code short[]}, {@code int[]}, {@code long[]}, {@code float[]}, {@code double[]},
   * {@code String[]}, and {@code List<String>}.
   *
   * <p> If strict parsing is enabled, options must start with '--', and not have an empty argument
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
        Preconditions.checkArgument(arg.startsWith("--"),
            "Argument '%s' does not begin with '--'", arg);
        int index = arg.indexOf("=");
        // Make sure that '=' isn't the first character after '--' or the last character
        Preconditions.checkArgument(index != 2,
            "Argument '%s' starts with '--=', empty argument name not allowed", arg);
        if (index > 0) {
          builder.put(arg.substring(2, index), arg.substring(index + 1, arg.length()));
        } else {
          builder.put(arg.substring(2), "true");
        }
      } catch (IllegalArgumentException e) {
        if (strictParsing) {
          throw e;
        } else {
          LOG.warn("Strict parsing is disabled, ignoring option '{}' because {}",
              arg, e.getMessage());
        }
      }
    }
    return builder.build();
  }

  /**
   * Using the parsed string arguments, we convert the strings to the expected
   * return type of the methods that are found on the passed-in class.
   * <p>
   * For any return type that is expected to be an array or a collection, we further
   * split up each string on ','.
   * <p>
   * We special case the "runner" option. It is mapped to the class of the {@link PipelineRunner}
   * based off of the {@link PipelineRunner}s simple class name.
   * <p>
   * If strict parsing is enabled, unknown options or options that cannot be converted to
   * the expected java type using an {@link ObjectMapper} will be ignored.
   */
  private static <T extends PipelineOptions> Map<String, Object> parseObjects(
      Class<T> klass, ListMultimap<String, String> options, boolean strictParsing) {
    Map<String, Method> propertyNamesToGetters = Maps.newHashMap();
    PipelineOptionsFactory.validateWellFormed(klass, REGISTERED_OPTIONS);
    @SuppressWarnings("unchecked")
    Iterable<PropertyDescriptor> propertyDescriptors =
        PipelineOptionsFactory.getPropertyDescriptors(
            FluentIterable.from(getRegisteredOptions()).append(klass).toSet());
    for (PropertyDescriptor descriptor : propertyDescriptors) {
      propertyNamesToGetters.put(descriptor.getName(), descriptor.getReadMethod());
    }
    Map<String, Object> convertedOptions = Maps.newHashMap();
    for (Map.Entry<String, Collection<String>> entry : options.asMap().entrySet()) {
      try {
        Preconditions.checkArgument(propertyNamesToGetters.containsKey(entry.getKey()),
            "Class %s missing a property named '%s'", klass, entry.getKey());

        Method method = propertyNamesToGetters.get(entry.getKey());
        // Only allow empty argument values for String, String Array, and Collection.
        Class<?> returnType = method.getReturnType();
        JavaType type = MAPPER.getTypeFactory().constructType(method.getGenericReturnType());
        if ("runner".equals(entry.getKey())) {
          String runner = Iterables.getOnlyElement(entry.getValue());
          Preconditions.checkArgument(SUPPORTED_PIPELINE_RUNNERS.containsKey(runner),
              "Unknown 'runner' specified '%s', supported pipeline runners %s",
              runner, Sets.newTreeSet(SUPPORTED_PIPELINE_RUNNERS.keySet()));
          convertedOptions.put("runner", SUPPORTED_PIPELINE_RUNNERS.get(runner));
        } else if (returnType.isArray() || Collection.class.isAssignableFrom(returnType)) {
          // Split any strings with ","
          List<String> values = FluentIterable.from(entry.getValue())
              .transformAndConcat(new Function<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(String input) {
                  return Arrays.asList(input.split(","));
                }
          }).toList();

          if (returnType.isArray() && !returnType.getComponentType().equals(String.class)) {
            for (String value : values) {
              Preconditions.checkArgument(!value.isEmpty(),
                  "Empty argument value is only allowed for String, String Array, and Collection,"
                  + " but received: " + returnType);
            }
          }
          convertedOptions.put(entry.getKey(), MAPPER.convertValue(values, type));
        } else {
          String value = Iterables.getOnlyElement(entry.getValue());
          Preconditions.checkArgument(returnType.equals(String.class) || !value.isEmpty(),
              "Empty argument value is only allowed for String, String Array, and Collection,"
               + " but received: " + returnType);
          convertedOptions.put(entry.getKey(), MAPPER.convertValue(value, type));
        }
      } catch (IllegalArgumentException e) {
        if (strictParsing) {
          throw e;
        } else {
          LOG.warn("Strict parsing is disabled, ignoring option '{}' with value '{}' because {}",
              entry.getKey(), entry.getValue(), e.getMessage());
        }
      }
    }
    return convertedOptions;
  }
}
