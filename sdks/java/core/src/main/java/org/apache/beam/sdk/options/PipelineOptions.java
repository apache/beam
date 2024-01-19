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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.ProxyInvocationHandler.Deserializer;
import org.apache.beam.sdk.options.ProxyInvocationHandler.Serializer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * PipelineOptions are used to configure Pipelines. You can extend {@link PipelineOptions} to create
 * custom configuration options specific to your {@link Pipeline}, for both local execution and
 * execution via a {@link PipelineRunner}.
 *
 * <p>{@link PipelineOptions} and their subinterfaces represent a collection of properties which can
 * be manipulated in a type safe manner. {@link PipelineOptions} is backed by a dynamic {@link
 * Proxy} which allows for type safe manipulation of properties in an extensible fashion through
 * plain old Java interfaces.
 *
 * <p>{@link PipelineOptions} can be created with {@link PipelineOptionsFactory#create()} and {@link
 * PipelineOptionsFactory#as(Class)}. They can be created from command-line arguments with {@link
 * PipelineOptionsFactory#fromArgs(String[])}. They can be converted to another type by invoking
 * {@link PipelineOptions#as(Class)} and can be accessed from within a {@link DoFn} by invoking
 * {@code getPipelineOptions()} on the input {@link DoFn.ProcessContext Context} object.
 *
 * <p>Please don't implement {@link PipelineOptions}, it implies that it is backwards-incompatible
 * to add new options. User-implemented {@link PipelineOptions} is not accepted by {@link Pipeline}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // The most common way to construct PipelineOptions is via command-line argument parsing:
 * public static void main(String[] args) {
 *   // Will parse the arguments passed into the application and construct a PipelineOptions
 *   // Note that --help will print registered options, and --help=PipelineOptionsClassName
 *   // will print out usage for the specific class.
 *   PipelineOptions options =
 *       PipelineOptionsFactory.fromArgs(args).create();
 *
 *   Pipeline p = Pipeline.create(options);
 *   ...
 *   p.run();
 * }
 *
 * // To create options for the DirectRunner:
 * DirectOptions directRunnerOptions =
 *     PipelineOptionsFactory.as(DirectOptions.class);
 *
 * // To cast from one type to another using the as(Class) method:
 * ApplicationNameOptions applicationNameOptions =
 *     directPipelineOptions.as(ApplicationNameOptions.class);
 *
 * // Options for the same property are shared between types
 * // The statement below will print out the name of the enclosing class by default
 * System.out.println(applicationNameOptions.getApplicationName());
 *
 * // Prints out registered options.
 * PipelineOptionsFactory.printHelp(System.out);
 *
 * // Prints out options which are available to be set on ApplicationNameOptions
 * PipelineOptionsFactory.printHelp(System.out, ApplicationNameOptions.class);
 * }</pre>
 *
 * <h2>Defining Your Own PipelineOptions</h2>
 *
 * <p>Defining your own {@link PipelineOptions} is the way for you to make configuration options
 * available for both local execution and execution via a {@link PipelineRunner}. By having
 * PipelineOptionsFactory as your command-line interpreter, you will provide a standardized way for
 * users to interact with your application via the command-line.
 *
 * <p>To define your own {@link PipelineOptions}, you create a public interface which extends {@link
 * PipelineOptions} and define getter/setter pairs. These getter/setter pairs define a collection of
 * <a href="https://docs.oracle.com/javase/tutorial/javabeans/writing/properties.html">JavaBean
 * properties</a>.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // Creates a user defined property called "myProperty"
 * public interface MyOptions extends PipelineOptions {
 *   String getMyProperty();
 *   void setMyProperty(String value);
 * }
 * }</pre>
 *
 * <p>Note: Please see the section on Registration below when using custom property types.
 *
 * <h3>Restrictions</h3>
 *
 * <p>Since PipelineOptions can be "cast" to multiple types dynamically using {@link
 * PipelineOptions#as(Class)}, a property must conform to the following set of restrictions:
 *
 * <ul>
 *   <li>Any property with the same name must have the same return type for all derived interfaces
 *       of {@link PipelineOptions}.
 *   <li>Every bean property of any interface derived from {@link PipelineOptions} must have a
 *       getter and setter method.
 *   <li>Every method must conform to being a getter or setter for a JavaBean.
 *   <li>The derived interface of {@link PipelineOptions} must be composable with every interface
 *       part registered with the PipelineOptionsFactory.
 *   <li>Only getters may be annotated with {@link JsonIgnore @JsonIgnore}.
 *   <li>If any getter is annotated with {@link JsonIgnore @JsonIgnore}, then all getters for this
 *       property must be annotated with {@link JsonIgnore @JsonIgnore}.
 *   <li>If any getter is annotated with {@link JsonDeserialize} and {@link JsonSerialize}, then all
 *       getters for this property must also be.
 * </ul>
 *
 * <h3>Annotations For PipelineOptions</h3>
 *
 * <p>{@link Description @Description} can be used to annotate an interface or a getter with useful
 * information which is output when {@code --help} is invoked via {@link
 * PipelineOptionsFactory#fromArgs(String[])}.
 *
 * <p>{@link Default @Default} represents a set of annotations that can be used to annotate getter
 * properties on {@link PipelineOptions} with information representing the default value to be
 * returned if no value is specified. Any default implementation (using the {@code default} keyword)
 * is ignored.
 *
 * <p>{@link Hidden @Hidden} hides an option from being listed when {@code --help} is invoked via
 * {@link PipelineOptionsFactory#fromArgs(String[])}.
 *
 * <p>{@link Validation @Validation} represents a set of annotations that can be used to annotate
 * getter properties on {@link PipelineOptions} with information representing the validation
 * criteria to be used when validating with the {@link PipelineOptionsValidator}. Validation will be
 * performed if during construction of the {@link PipelineOptions}, {@link
 * PipelineOptionsFactory#withValidation()} is invoked.
 *
 * <p>{@link JsonIgnore @JsonIgnore} is used to prevent a property from being serialized and
 * available during execution of {@link DoFn}. See the Serialization section below for more details.
 *
 * <p>{@link JsonSerialize @JsonSerialize} and {@link JsonDeserialize @JsonDeserialize} is used to
 * control how a property is (de)serialized when the PipelineOptions are (de)serialized to JSON. See
 * the Serialization section below for more details.
 *
 * <h2>Registration Of PipelineOptions</h2>
 *
 * <p>Registration of {@link PipelineOptions} by an application guarantees that the {@link
 * PipelineOptions} is composable during execution of their {@link Pipeline} and meets the
 * restrictions listed above or will fail during registration. Registration also lists the
 * registered {@link PipelineOptions} when {@code --help} is invoked via {@link
 * PipelineOptionsFactory#fromArgs(String[])}.
 *
 * <p>Registration can be performed by invoking {@link PipelineOptionsFactory#register} within a
 * users application or via automatic registration by creating a {@link ServiceLoader} entry and a
 * concrete implementation of the {@link PipelineOptionsRegistrar} interface.
 *
 * <p>It is optional but recommended to use one of the many build time tools such as {@link
 * AutoService} to generate the necessary META-INF files automatically.
 *
 * <p>A list of registered options can be fetched from {@link
 * PipelineOptionsFactory#getRegisteredOptions()}.
 *
 * <h2>Serialization Of PipelineOptions</h2>
 *
 * {@link PipelineOptions} is intentionally <i>not</i> marked {@link java.io.Serializable}, in order
 * to discourage pipeline authors from capturing {@link PipelineOptions} at pipeline construction
 * time, because a pipeline may be saved as a template and run with a different set of options than
 * the ones it was constructed with. See {@link Pipeline#run(PipelineOptions)}.
 *
 * <p>However, {@link PipelineRunner}s require support for options to be serialized. Each property
 * within {@link PipelineOptions} must be able to be serialized using Jackson's {@link ObjectMapper}
 * or the getter method for the property annotated with {@link JsonIgnore @JsonIgnore}.
 *
 * <p>Jackson supports serialization of many types and supports a useful set of <a
 * href="https://github.com/FasterXML/jackson-annotations">annotations</a> to aid in serialization
 * of custom types. We point you to the public <a
 * href="https://github.com/FasterXML/jackson">Jackson documentation</a> when attempting to add
 * serialization support for your custom types. Note that {@link PipelineOptions} relies on
 * Jackson's ability to automatically configure the {@link ObjectMapper} with additional modules via
 * {@link ObjectMapper#findModules()}.
 *
 * <p>To further customize serialization, getter methods may be annotated with {@link
 * JsonSerialize @JsonSerialize} and {@link JsonDeserialize @JsonDeserialize}. {@link
 * JsonDeserialize @JsonDeserialize} is also used when parsing command line arguments.
 *
 * <p>Note: A property must be annotated with <b>BOTH</b>{@link JsonDeserialize @JsonDeserialize}
 * and {@link JsonSerialize @JsonSerialize} or neither. It is an error to have a property annotated
 * with only {@link JsonDeserialize @JsonDeserialize} or {@link JsonSerialize @JsonSerialize}.
 *
 * <p>Note: It is an error to have the same property available in multiple interfaces with only some
 * of them being annotated with {@link JsonIgnore @JsonIgnore}. It is also an error to mark a setter
 * for a property with {@link JsonIgnore @JsonIgnore}.
 */
@JsonSerialize(using = Serializer.class)
@JsonDeserialize(using = Deserializer.class)
@ThreadSafe
public interface PipelineOptions extends HasDisplayData {
  /**
   * Transforms this object into an object of type {@code <T>} saving each property that has been
   * manipulated. {@code <T>} must extend {@link PipelineOptions}.
   *
   * <p>If {@code <T>} is not registered with the {@link PipelineOptionsFactory}, then we attempt to
   * verify that {@code <T>} is composable with every interface that this instance of the {@code
   * PipelineOptions} has seen.
   *
   * @param kls The class of the type to transform to.
   * @return An object of type kls.
   */
  <T extends PipelineOptions> T as(Class<T> kls);

  /**
   * The pipeline runner that will be used to execute the pipeline. For registered runners, the
   * class name can be specified, otherwise the fully qualified name needs to be specified.
   */
  @Validation.Required
  @Description(
      "The pipeline runner that will be used to execute the pipeline. "
          + "For registered runners, the class name can be specified, otherwise the fully "
          + "qualified name needs to be specified.")
  @Default.InstanceFactory(DirectRunner.class)
  Class<? extends PipelineRunner<?>> getRunner();

  void setRunner(Class<? extends PipelineRunner<?>> kls);

  /** Enumeration of the possible states for a given check. */
  enum CheckEnabled {
    OFF,
    WARNING,
    ERROR
  }

  /**
   * Whether to check for stable unique names on each transform. This is necessary to support
   * updating of pipelines.
   */
  @Validation.Required
  @Description(
      "Whether to check for stable unique names on each transform. This is necessary to "
          + "support updating of pipelines.")
  @Default.Enum("WARNING")
  CheckEnabled getStableUniqueNames();

  void setStableUniqueNames(CheckEnabled enabled);

  /**
   * A pipeline level default location for storing temporary files.
   *
   * <p>This can be a path of any file system.
   *
   * <p>{@link #getTempLocation()} can be used as a default location in other {@link
   * PipelineOptions}.
   *
   * <p>If it is unset, {@link PipelineRunner} can override it.
   */
  @Description("A pipeline level default location for storing temporary files.")
  String getTempLocation();

  void setTempLocation(String value);

  @Description(
      "Name of the pipeline execution."
          + "It must match the regular expression '[a-z]([-a-z0-9]{0,1022}[a-z0-9])?'."
          + "It defaults to ApplicationName-UserName-Date-RandomInteger")
  @Default.InstanceFactory(JobNameFactory.class)
  String getJobName();

  void setJobName(String jobName);

  /**
   * A {@link DefaultValueFactory} that obtains the class of the {@code DirectRunner} if it exists
   * on the classpath, and throws an exception otherwise.
   *
   * <p>As the {@code DirectRunner} is in an independent module, it cannot be directly referenced as
   * the {@link Default}. However, it should still be used if available, and a user is required to
   * explicitly set the {@code --runner} property if they wish to use an alternative runner.
   */
  class DirectRunner implements DefaultValueFactory<Class<? extends PipelineRunner<?>>> {
    @Override
    public Class<? extends PipelineRunner<?>> create(PipelineOptions options) {
      try {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<? extends PipelineRunner<?>> direct =
            (Class<? extends PipelineRunner<?>>)
                Class.forName(
                    "org.apache.beam.runners.direct.DirectRunner",
                    true,
                    ReflectHelpers.findClassLoader());
        return direct;
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            String.format(
                "No Runner was specified and the DirectRunner was not found on the classpath.%n"
                    + "Specify a runner by either:%n"
                    + "    Explicitly specifying a runner by providing the 'runner' property%n"
                    + "    Adding the DirectRunner to the classpath%n"
                    + "    Calling 'PipelineOptions.setRunner(PipelineRunner)' directly"));
      }
    }
  }

  /**
   * Returns a normalized job name constructed from {@link ApplicationNameOptions#getAppName()}, the
   * local system user name (if available), the current time, and a random integer.
   *
   * <p>The normalization makes sure that the name matches the pattern of
   * [a-z]([-a-z0-9]*[a-z0-9])?.
   */
  class JobNameFactory implements DefaultValueFactory<String> {
    private static final DateTimeFormatter FORMATTER =
        DateTimeFormat.forPattern("MMddHHmmss").withZone(DateTimeZone.UTC);

    @Override
    public String create(PipelineOptions options) {
      String appName = options.as(ApplicationNameOptions.class).getAppName();
      String normalizedAppName =
          appName == null || appName.length() == 0
              ? "BeamApp"
              : appName.toLowerCase().replaceAll("[^a-z0-9]", "0").replaceAll("^[^a-z]", "a");
      String userName = MoreObjects.firstNonNull(System.getProperty("user.name"), "");
      String normalizedUserName = userName.toLowerCase().replaceAll("[^a-z0-9]", "0");
      String datePart = FORMATTER.print(DateTimeUtils.currentTimeMillis());

      String randomPart = Integer.toHexString(ThreadLocalRandom.current().nextInt());
      return String.format(
          "%s-%s-%s-%s", normalizedAppName, normalizedUserName, datePart, randomPart);
    }
  }

  /**
   * Returns a map of properties which correspond to {@link ValueProvider.RuntimeValueProvider},
   * keyed by the property name. The value is a map containing type and default information.
   */
  Map<String, Map<String, Object>> outputRuntimeOptions();

  /**
   * A monotonically increasing revision number of this {@link PipelineOptions} object that can be
   * used to detect changes.
   */
  int revision();

  /**
   * Provides a process wide unique ID for this {@link PipelineOptions} object, assigned at graph
   * construction time.
   */
  @Hidden
  @Default.InstanceFactory(AtomicLongFactory.class)
  long getOptionsId();

  void setOptionsId(long id);

  /**
   * {@link DefaultValueFactory} which supplies an ID that is guaranteed to be unique within the
   * given process.
   */
  class AtomicLongFactory implements DefaultValueFactory<Long> {
    private static final AtomicLong NEXT_ID = new AtomicLong(0);

    @Override
    public Long create(PipelineOptions options) {
      return NEXT_ID.getAndIncrement();
    }
  }

  /**
   * A user agent string as per RFC2616, describing the pipeline to external services.
   *
   * <p>https://www.ietf.org/rfc/rfc2616.txt
   *
   * <p>It should follow the BNF Form:
   *
   * <pre><code>
   * user agent         = 1*(product | comment)
   * product            = token ["/" product-version]
   * product-version    = token
   * </code></pre>
   *
   * Where a token is a series of characters without a separator.
   *
   * <p>The string defaults to {@code [name]/[version]} based on the properties of the Apache Beam
   * release.
   */
  @Description(
      "A user agent string describing the pipeline to external services."
          + " The format should follow RFC2616. This option defaults to \"[name]/[version]\""
          + " where name and version are properties of the Apache Beam release.")
  @Default.InstanceFactory(UserAgentFactory.class)
  String getUserAgent();

  void setUserAgent(String userAgent);

  /**
   * Returns a user agent string constructed from {@link ReleaseInfo#getName()} and {@link
   * ReleaseInfo#getVersion()}, in the format {@code [name]/[version]}.
   */
  class UserAgentFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      ReleaseInfo info = ReleaseInfo.getReleaseInfo();
      return String.format("%s/%s", info.getName(), info.getVersion()).replace(" ", "_");
    }
  }
}
