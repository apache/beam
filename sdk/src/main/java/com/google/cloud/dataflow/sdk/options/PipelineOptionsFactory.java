/*
 * Copyright (C) 2014 Google Inc.
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
import com.google.common.base.Equivalence;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
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
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * Constructs a {@link PipelineOptions} or any derived interface which is composable to any other
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
   * Creates and returns an object which implements {@link PipelineOptions}.
   * This sets the {@link ApplicationNameOptions#getAppName() "appName"} to the calling
   * {@link Class#getSimpleName() classes simple name}.
   *
   * @return An object which implements {@link PipelineOptions}.
   */
  public static PipelineOptions create() {
    return new Builder().as(PipelineOptions.class);
  }

  /**
   * Creates and returns an object which implements {@code <T>}.
   * This sets the {@link ApplicationNameOptions#getAppName() "appName"} to the calling
   * {@link Class#getSimpleName() classes simple name}.
   *
   * <p> Note that {@code <T>} must be composable with every registered interface with this factory.
   * See {@link PipelineOptionsFactory#validateWellFormed(Class, Set)} for more details.
   *
   * @return An object which implements {@code <T>}.
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
   * {@link Builder#withoutStrictParsing()}.
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

  /** A fluent PipelineOptions builder. */
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
     * {@link Builder#withoutStrictParsing()}.
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
     * Creates and returns an object which implements {@link PipelineOptions} using the values
     * configured on this builder during construction.
     *
     * @return An object which implements {@link PipelineOptions}.
     */
    public PipelineOptions create() {
      return as(PipelineOptions.class);
    }

    /**
     * Creates and returns an object which implements {@code <T>} using the values configured on
     * this builder during construction.
     * <p>
     * Note that {@code <T>} must be composable with every registered interface with this factory.
     * See {@link PipelineOptionsFactory#validateWellFormed(Class, Set)} for more details.
     *
     * @return An object which implements {@code <T>}.
     */
    public <T extends PipelineOptions> T as(Class<T> klass) {
      Map<String, Object> initialOptions = Maps.newHashMap();

      // Attempt to parse the arguments into the set of initial options to use
      if (args != null) {
        ListMultimap<String, String> options = parseCommandLine(args, strictParsing);
        LOG.debug("Provided Arguments: {}", options);
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
    // Then find the first instance after which is not the PipelineOptionsFactory/Builder class.
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

  /** Classes which are used as the boundary in the stack trace to find the callers class name. */
  private static final Set<String> PIPELINE_OPTIONS_FACTORY_CLASSES = ImmutableSet.of(
      PipelineOptionsFactory.class.getName(),
      Builder.class.getName());

  /** Methods which are ignored when validating the proxy class. */
  private static final Set<Method> IGNORED_METHODS;

  /** The set of options which have been registered and visible to the user. */
  private static final Set<Class<? extends PipelineOptions>> REGISTERED_OPTIONS =
      Sets.newConcurrentHashSet();

  /** A cache storing a mapping from a given interface to its registration record. */
  private static final Map<Class<? extends PipelineOptions>, Registration<?>> INTERFACE_CACHE =
      Maps.newConcurrentMap();

  /** A cache storing a mapping from a set of interfaces to its registration record. */
  private static final Map<Set<Class<? extends PipelineOptions>>, Registration<?>> COMBINED_CACHE =
      Maps.newConcurrentMap();

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

    // Store the list of all available pipeline runners.
    ImmutableMap.Builder<String, Class<? extends PipelineRunner<?>>> builder =
            ImmutableMap.builder();
    Set<PipelineRunnerRegistrar> pipelineRunnerRegistrars =
        Sets.newTreeSet(ObjectsClassComparator.INSTANCE);
    pipelineRunnerRegistrars.addAll(
        Lists.newArrayList(ServiceLoader.load(PipelineRunnerRegistrar.class)));
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
        Lists.newArrayList(ServiceLoader.load(PipelineOptionsRegistrar.class)));
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
            getPropertyDescriptors(allProxyClass);
        validateClass(iface, validatedPipelineOptionsInterfaces,
            allProxyClass, propertyDescriptors);
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
            getPropertyDescriptors(proxyClass);
        validateClass(iface, validatedPipelineOptionsInterfaces, proxyClass, propertyDescriptors);
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
   * @return A {@link DataflowWorkerHarnessOptions} object configured for the
   *         {@link DataflowWorkerHarness}.
   */
  @Deprecated
  public static DataflowWorkerHarnessOptions createFromSystemProperties() {
    DataflowWorkerHarnessOptions options = as(DataflowWorkerHarnessOptions.class);
    options.setRunner(null);
    if (System.getProperties().containsKey("root_url")) {
      options.setApiRootUrl(System.getProperty("root_url"));
    }
    if (System.getProperties().containsKey("service_path")) {
      options.setDataflowEndpoint(System.getProperty("service_path"));
    }
    if (System.getProperties().containsKey("temp_gcs_directory")) {
      options.setTempLocation(System.getProperty("temp_gcs_directory"));
    }
    if (System.getProperties().containsKey("service_account_name")) {
      options.setServiceAccountName(System.getProperty("service_account_name"));
    }
    if (System.getProperties().containsKey("service_account_keyfile")) {
      options.setServiceAccountKeyfile(System.getProperty("service_account_keyfile"));
    }
    if (System.getProperties().containsKey("worker_id")) {
      options.setWorkerId(System.getProperty("worker_id"));
    }
    if (System.getProperties().containsKey("project_id")) {
      options.setProject(System.getProperty("project_id"));
    }
    if (System.getProperties().containsKey("job_id")) {
      options.setJobId(System.getProperty("job_id"));
    }
    if (System.getProperties().containsKey("path_validator_class")) {
      try {
        options.setPathValidatorClass((Class) Class.forName(
            System.getProperty("path_validator_class")));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Unable to find validator class", e);
      }
    }
    if (System.getProperties().containsKey("credential_factory_class")) {
      try {
        options.setCredentialFactoryClass((Class) Class.forName(
            System.getProperty("credential_factory_class")));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Unable to find credential factory class", e);
      }
    }
    return options;
  }

  /**
   * Returns all the methods visible from the provided interfaces.
   *
   * @param interfaces The interfaces to use when searching for all their methods.
   * @return An iterable of {@link Method}s which interfaces expose.
   */
  static Iterable<Method> getClosureOfMethodsOnInterfaces(
      Iterable<Class<? extends PipelineOptions>> interfaces) {
    return FluentIterable.from(interfaces).transformAndConcat(
        new Function<Class<? extends PipelineOptions>, Iterable<Method>>() {
          @Override
          public Iterable<Method> apply(Class<? extends PipelineOptions> input) {
            return getClosureOfMethodsOnInterface(input);
          }
    });
  }

  /**
   * Returns all the methods visible from {@code iface}.
   *
   * @param iface The interface to use when searching for all its methods.
   * @return An iterable of {@link Method}s which {@code iface} exposes.
   */
  static Iterable<Method> getClosureOfMethodsOnInterface(Class<? extends PipelineOptions> iface) {
    Preconditions.checkNotNull(iface);
    Preconditions.checkArgument(iface.isInterface());
    ImmutableList.Builder<Method> builder = ImmutableList.builder();
    Queue<Class<?>> interfacesToProcess = Queues.newArrayDeque();
    interfacesToProcess.add(iface);
    while (!interfacesToProcess.isEmpty()) {
      Class<?> current = interfacesToProcess.remove();
      builder.add(current.getMethods());
      interfacesToProcess.addAll(Arrays.asList(current.getInterfaces()));
    }
    return builder.build();
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
    // Build a map of property names to getters.
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
      descriptors.add(new PropertyDescriptor(
          propertyName, propertyNamesToGetters.remove(propertyName), method));
    }

    // Add the remaining getters with missing setters.
    for (Map.Entry<String, Method> getterToMethod : propertyNamesToGetters.entrySet()) {
      descriptors.add(new PropertyDescriptor(
          getterToMethod.getKey(), getterToMethod.getValue(), null));
    }
    return descriptors;
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
   * @param descriptors A list of {@link PropertyDescriptor}s to use when validating.
   */
  private static void validateClass(Class<? extends PipelineOptions> iface,
      Set<Class<? extends PipelineOptions>> validatedPipelineOptionsInterfaces,
      Class<?> klass, List<PropertyDescriptor> descriptors) {
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
    } catch (NoSuchMethodException | SecurityException e) {
      throw Throwables.propagate(e);
    }

    // Verify that there are no methods with the same name with two different return types.
    Iterable<Method> interfaceMethods = FluentIterable
        .from(getClosureOfMethodsOnInterface(iface))
        .toSortedSet(MethodComparator.INSTANCE);
    SetMultimap<Equivalence.Wrapper<Method>, Method> methodNameToMethodMap =
        HashMultimap.create();
    for (Method method : interfaceMethods) {
      methodNameToMethodMap.put(MethodNameEquivalence.INSTANCE.wrap(method), method);
    }
    for (Map.Entry<Equivalence.Wrapper<Method>, Collection<Method>> entry
        : methodNameToMethodMap.asMap().entrySet()) {
      Set<Class<?>> returnTypes = FluentIterable.from(entry.getValue())
          .transform(ReturnTypeFetchingFunction.INSTANCE).toSet();
      SortedSet<Method> collidingMethods = FluentIterable.from(entry.getValue())
          .toSortedSet(MethodComparator.INSTANCE);
      Preconditions.checkArgument(returnTypes.size() == 1,
          "Method [%s] has multiple definitions %s with different return types for [%s].",
          entry.getKey().get().getName(),
          collidingMethods,
          iface.getName());
    }

    // Verify that there is no getter with a mixed @JsonIgnore annotation and verify
    // that no setter has @JsonIgnore.
    Iterable<Method> allInterfaceMethods = FluentIterable
        .from(getClosureOfMethodsOnInterfaces(validatedPipelineOptionsInterfaces))
        .append(getClosureOfMethodsOnInterface(iface))
        .toSortedSet(MethodComparator.INSTANCE);
    SetMultimap<Equivalence.Wrapper<Method>, Method> methodNameToAllMethodMap =
        HashMultimap.create();
    for (Method method : allInterfaceMethods) {
      methodNameToAllMethodMap.put(MethodNameEquivalence.INSTANCE.wrap(method), method);
    }
    for (PropertyDescriptor descriptor : descriptors) {
      if (IGNORED_METHODS.contains(descriptor.getReadMethod())
          || IGNORED_METHODS.contains(descriptor.getWriteMethod())) {
        continue;
      }
      Set<Method> getters =
          methodNameToAllMethodMap.get(
              MethodNameEquivalence.INSTANCE.wrap(descriptor.getReadMethod()));
      Set<Method> gettersWithJsonIgnore =
          FluentIterable.from(getters).filter(JsonIgnorePredicate.INSTANCE).toSet();

      Iterable<String> getterClassNames = FluentIterable.from(getters)
          .transform(MethodToDeclaringClassFunction.INSTANCE)
          .transform(ClassNameFunction.INSTANCE);
      Iterable<String> gettersWithJsonIgnoreClassNames = FluentIterable.from(gettersWithJsonIgnore)
          .transform(MethodToDeclaringClassFunction.INSTANCE)
          .transform(ClassNameFunction.INSTANCE);

      Preconditions.checkArgument(gettersWithJsonIgnore.isEmpty()
          || getters.size() == gettersWithJsonIgnore.size(),
          "Expected getter for property [%s] to be marked with @JsonIgnore on all %s, "
          + "found only on %s",
          descriptor.getName(), getterClassNames, gettersWithJsonIgnoreClassNames);

      Set<Method> settersWithJsonIgnore = FluentIterable.from(
          methodNameToAllMethodMap.get(
              MethodNameEquivalence.INSTANCE.wrap(descriptor.getWriteMethod())))
                  .filter(JsonIgnorePredicate.INSTANCE).toSet();

      Iterable<String> settersWithJsonIgnoreClassNames = FluentIterable.from(settersWithJsonIgnore)
          .transform(MethodToDeclaringClassFunction.INSTANCE)
          .transform(ClassNameFunction.INSTANCE);

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
    Set<Method> unknownMethods = Sets.difference(Sets.newHashSet(klass.getMethods()), methods);
    Preconditions.checkArgument(unknownMethods.isEmpty(),
        "Methods %s on [%s] do not conform to being bean properties.",
        FluentIterable.from(unknownMethods).transform(MethodFormatterFunction.INSTANCE),
        iface.getName());
  }

  /** A {@link Comparator} which uses the classes canonical name to compare them. */
  private static class ObjectsClassComparator implements Comparator<Object> {
    static final ObjectsClassComparator INSTANCE = new ObjectsClassComparator();
    @Override
    public int compare(Object o1, Object o2) {
      return o1.getClass().getCanonicalName().compareTo(o2.getClass().getCanonicalName());
    }
  }

  /** A {@link Comparator} which uses the generic method signature to sort them. */
  private static class MethodComparator implements Comparator<Method> {
    static final MethodComparator INSTANCE = new MethodComparator();
    @Override
    public int compare(Method o1, Method o2) {
      return o1.toGenericString().compareTo(o2.toGenericString());
    }
  }

  /** A {@link Function} which gets the methods return type. */
  private static class ReturnTypeFetchingFunction implements Function<Method, Class<?>> {
    static final ReturnTypeFetchingFunction INSTANCE = new ReturnTypeFetchingFunction();
    @Override
    public Class<?> apply(Method input) {
      return input.getReturnType();
    }
  }

  /** A {@link Function} which turns a method into a simple method signature. */
  private static class MethodFormatterFunction implements Function<Method, String> {
    static final MethodFormatterFunction INSTANCE = new MethodFormatterFunction();
    @Override
    public String apply(Method input) {
      String parameterTypes = FluentIterable.of(input.getParameterTypes())
          .transform(ClassNameFunction.INSTANCE)
          .toSortedList(String.CASE_INSENSITIVE_ORDER)
          .toString();
      return ClassNameFunction.INSTANCE.apply(input.getReturnType()) + " " + input.getName()
          + "(" + parameterTypes.substring(1, parameterTypes.length() - 1) + ")";
    }
  }

  /** A {@link Function} with returns the classes name. */
  private static class ClassNameFunction implements Function<Class<?>, String> {
    static final ClassNameFunction INSTANCE = new ClassNameFunction();
    @Override
    public String apply(Class<?> input) {
      return input.getName();
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

  /** An {@link Equivalence} which considers two methods equivalent if they share the same name. */
  private static class MethodNameEquivalence extends Equivalence<Method> {
    static final MethodNameEquivalence INSTANCE = new MethodNameEquivalence();
    @Override
    protected boolean doEquivalent(Method a, Method b) {
      return a.getName().equals(b.getName());
    }

    @Override
    protected int doHash(Method t) {
      return t.getName().hashCode();
    }
  }

  /**
   * A {@link Predicate} which returns true if the method is annotated with
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
   * name or value based upon the positioning of the '='.
   */
  private static ListMultimap<String, String> parseCommandLine(
      String[] args, boolean strictParsing) {
    ImmutableListMultimap.Builder<String, String> builder = ImmutableListMultimap.builder();
    for (String arg : args) {
      try {
        Preconditions.checkArgument(arg.startsWith("--"),
            "Argument '%s' does not begin with '--'", arg);
        int index = arg.indexOf("=");
        // Make sure that '=' isn't the first character after '--' or the last character
        Preconditions.checkArgument(index != 2,
            "Argument '%s' starts with '--=', empty argument name not allowed", arg);
        Preconditions.checkArgument(index != arg.length() - 1,
            "Argument '%s' ends with '=', empty argument value not allowed", arg);
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
   * return type of the methods which are found on the passed in class.
   * <p>
   * For any return type that is expected to be an array or a collection, we further
   * split up each string on ','.
   * <p>
   * We special case the "runner" option. It is mapped to the class of the {@link PipelineRunner}
   * based off of the {@link PipelineRunner}s simple class name.
   * <p>
   * If strict parsing is enabled, unknown options or options which can not be converted to
   * the expected java type using an {@link ObjectMapper} will be ignored.
   */
  private static <T extends PipelineOptions> Map<String, Object> parseObjects(
      Class<T> klass, ListMultimap<String, String> options, boolean strictParsing) {
    Map<String, Method> propertyNamesToGetters = Maps.newHashMap();
    PipelineOptionsFactory.validateWellFormed(klass, getRegisteredOptions());
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
        JavaType type = MAPPER.getTypeFactory().constructType(method.getGenericReturnType());
        if ("runner".equals(entry.getKey())) {
          String runner = Iterables.getOnlyElement(entry.getValue());
          Preconditions.checkArgument(SUPPORTED_PIPELINE_RUNNERS.containsKey(runner),
              "Unknown 'runner' specified '%s', supported pipeline runners %s",
              runner, Sets.newTreeSet(SUPPORTED_PIPELINE_RUNNERS.keySet()));
          convertedOptions.put("runner", SUPPORTED_PIPELINE_RUNNERS.get(runner));
        } else if (method.getReturnType().isArray()
            || Collection.class.isAssignableFrom(method.getReturnType())) {
          // Split any strings with ","
          List<String> values = FluentIterable.from(entry.getValue())
              .transformAndConcat(new Function<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(String input) {
                  return Arrays.asList(input.split(","));
                }
          }).toList();
          convertedOptions.put(entry.getKey(), MAPPER.convertValue(values, type));
        } else {
          String value = Iterables.getOnlyElement(entry.getValue());
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
