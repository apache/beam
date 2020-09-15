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

import CommonTestProperties.Runner
import CommonTestProperties.SDK
import LoadTestConfig.SerializableOption
import groovy.json.JsonBuilder
import org.codehaus.groovy.runtime.InvokerHelper

import java.util.function.Predicate

import static java.util.Objects.nonNull
import static java.util.Objects.requireNonNull

/**
 * This class contains simple DSL for load tests configuration. Configuration as Map<String, Serializable>
 * [{@link LoadTestConfig#config config} -- returns configuration map]
 * [{@link LoadTestConfig#templateConfig templateConfig} -- return LoadTestConfig reusable object]
 * [{@link LoadTestConfig#fromTemplate fromTemplate} -- returns configuration from given template].<br><br>
 *
 * Example:
 * <blockquote><pre>
 * LoadTestConfig template = templateConfig {
 *     title 'Load test'
 *     test 'org.apache.beam.sdk.loadtests.SomeLoadTests'
 *     dataflow()
 *     pipelineOptions {
 *         python()
 *         jobName 'Any job name'
 *         publishToBigQuery true
 *         //other fields
 *     }
 *     specificParameters([
 *          fanout: 4
 *     ])
 * }
 * Map<String, Serializable> configMap = fromTemplate(template) {
 *     //fields can be changed or/and added
 *     portable()
 *     pipelineOptions {
 *         parallelism 5
 *         inputOptions {
 *             numRecords 20000
 *             keySize 1000
 *             valueSize 10
 *         }
 *     }
 * }
 * </pre></blockquote>
 */
class LoadTestConfig implements SerializableOption<Map<String, Serializable>> {

  private String _title
  private String _test
  private Runner _runner
  private PipelineOptions _pipelineOptions

  private LoadTestConfig() {}

  void title(final String title) {
    _title = title
  }
  void test(final String test) {
    _test = test
  }

  //runners
  void dataflow() { setRunnerAndUpdatePipelineOptions(Runner.DATAFLOW)}
  void portable() { setRunnerAndUpdatePipelineOptions(Runner.PORTABLE) }

  private void setRunnerAndUpdatePipelineOptions(final Runner runner) {
    _runner = runner
    final def pipeline = _pipelineOptions ?: new PipelineOptions()
    pipeline.i_runner = runner
    _pipelineOptions = pipeline
  }

  void pipelineOptions(final Closure cl = {}) {
    final def options = _pipelineOptions ?: new PipelineOptions()
    delegateAndInvoke(options, cl)
    _pipelineOptions = options
  }

  /**
   * Returns load test config object which can be reusable.</br>
   * All possible fields that can be set:
   * <blockquote><pre>
   * templateConfig {
   *     title        [String]
   *     test         [String]
   *     [dataflow(), portable()] -- runner
   *     pipelineOptions {
   *         [python(), python37(), java()] -- sdk
   *         jobName                  [String]
   *         appName                  [String]
   *         project                  [String]
   *         publishToBigQuery        [boolean]
   *         metricsDataset (python)  [String]
   *         metricsTable (python)    [String]
   *         bigQueryDataset (java)   [String]
   *         bigQueryTable (java)     [String]
   *         numWorkers               [int]
   *         parallelism              [int]
   *         tempLocation             [String]
   *         autoscalingAlgorithm     [String]
   *         jobEndpoint              [String]
   *         environmentType          [String]
   *         environmentConfig        [String]
   *         inputOptions/coInputOptions (for python) {
   *             numRecords           [int]
   *             keySize              [int]
   *             valueSize            [int]
   *             numHotKeys           [int]
   *             hotKeyFraction       [int]
   *         }
   *         sourceOptions/coSourceOptions (for java) {
   *             numRecords           [int]
   *             keySizeBytes         [int]
   *             valueSizeBytes       [int]
   *             numHotKeys           [int]
   *             hotKeyFraction       [int]
   *             splitPointFrequencyRecords       [int]
   *         }
   *         stepOptions {
   *             outputRecordsPerInputRecord      [int]
   *             preservesInputKeyDistribution    [boolean]
   *         }
   *         specificParameters       [Map<String, Object>]
   *     }
   * }
   * </pre></blockquote>
   * @param cl Closure with fields setting
   * @return LoadTestConfig object
   */
  static LoadTestConfig templateConfig(final Closure cl = {}) {
    final def config = new LoadTestConfig()
    delegateAndInvoke(config, cl)
    return config
  }

  /**
   * Returns configuration map from given template. Any field can be changed or/and added. Validation is performed
   * before final map is returned (ex. Flink runner requires <b>environmentConfig</b> to be set). In case of
   * validation failure exception is thrown.<br>
   * Example result:
   *<blockquote><pre>
   * [
   *  title          : 'any given title',
   *  test           : 'org.apache.beam.sdk.loadtests.SomeLoadTests',
   *  runner         : CommonTestProperties.Runner.DATAFLOW,
   *  pipelineOptions: [
   *    job_name            : 'any given job name',
   *    publish_to_big_query: true,
   *    project             : 'apache-beam-testing',
   *    metrics_dataset     : 'given_dataset_name',
   *    metrics_table       : 'given_table_name',
   *    input_options       : '\'{"num_records": 200000000,"key_size": 1,"value_size":9}\'',
   *    iterations          : 1,
   *    fanout              : 1,
   *    parallelism         : 5,
   *    job_endpoint        : 'localhost:1234',
   *    environment_config  : 'given_environment_config',
   *    environment_type    : 'given_environment_type'
   *  ]
   * ]
   * </blockquote></pre>
   * @param templateConfig LoadTestConfig instance
   * @param cl Closure with fields setting
   * @return configuration map
   * @see LoadTestConfig
   * @see LoadTestConfig#templateConfig
   */
  static Map<String, Serializable> fromTemplate(final LoadTestConfig templateConfig, final Closure cl = {}) {
    final def newConfig = of(templateConfig)
    delegateAndInvoke(newConfig, cl)
    final def properties = newConfig.propertiesMap
    verifyProperties(properties)
    return ConfigHelper.convertProperties(properties)
  }

  /**
   * Returns configuration map (see {@link LoadTestConfig#fromTemplate}) directly from given settings
   * @param cl Closure with settings
   * @return configuration map
   */
  static Map<String, Serializable> config(final Closure cl = {}) {
    final def config = new LoadTestConfig()
    delegateAndInvoke(config, cl)
    final def properties = config.propertiesMap
    verifyProperties(properties)
    return ConfigHelper.convertProperties(config.propertiesMap)
  }

  private static void delegateAndInvoke(final delegate, final Closure cl = {}) {
    final def code = cl.rehydrate(delegate, this, this)
    code.resolveStrategy = Closure.DELEGATE_ONLY
    code()
  }

  private static LoadTestConfig of(final LoadTestConfig oldConfig) {
    final def newConfig = new LoadTestConfig()

    //primitive values
    InvokerHelper.setProperties(newConfig, oldConfig.propertiesMap)

    //non-primitive values
    newConfig._pipelineOptions = oldConfig._pipelineOptions ? PipelineOptions.of(oldConfig._pipelineOptions) : null

    return newConfig
  }

  @Override
  Map<String, Serializable> toPrimitiveValues() {
    final def map = propertiesMap
    verifyProperties(map)
    return ConfigHelper.convertProperties(map)
  }

  LinkedHashMap<String, Object> getPropertiesMap() {
    return [
      _title: _title,
      _test: _test,
      _runner: _runner,
      _pipelineOptions: _pipelineOptions
    ]
  }

  private static void verifyProperties(final LinkedHashMap<String, Object> map) {
    for (entry in map.entrySet()) {
      requireNonNull(entry.value, "Missing ${entry.key.substring(1)} in configuration")
    }
  }

  private static class PipelineOptions implements SerializableOption<Map<String, Serializable>> {
    private Map<String, Object> _specificParameters = new HashMap<>()
    private boolean _streaming = false
    private SourceOptions _coSourceOptions
    private InputOptions _coInputOptions
    private StepOptions _stepOptions

    //required
    private String _project
    private String _publishToBigQuery

    //java required
    private String _bigQueryDataset
    private String _bigQueryTable
    private String _appName
    private SourceOptions _sourceOptions

    //python required
    private String _metricsDataset
    private String _metricsTable
    private String _jobName
    private InputOptions _inputOptions

    //internal usage
    private SDK i_sdk
    private Runner i_runner
    private static final i_required = [
      "_project",
      "_publishToBigQuery"
    ]
    private static final i_dataflowRequired = [
      "_numWorkers",
      "_tempLocation",
      "_autoscalingAlgorithm",
      "_region"
    ]
    private static final i_portableRequired = [
      "_jobEndpoint",
      "_environmentType",
      "_environmentConfig",
      "_parallelism"
    ]
    private static final i_javaRequired = [
      "_bigQueryDataset",
      "_bigQueryTable",
      "_sourceOptions",
      "_appName"
    ]
    private static final i_pythonRequired = [
      "_metricsDataset",
      "_metricsTable",
      "_inputOptions",
      "_jobName"
    ]

    //dataflow required
    private def  _numWorkers
    private String  _tempLocation
    private String  _autoscalingAlgorithm
    private String _region = 'us-central1'

    //flink required
    private String _jobEndpoint
    private String _environmentType
    private String _environmentConfig
    private def _parallelism

    void jobName(final String name) { _jobName = name }
    void appName(final String name) { _appName = name }
    void project(final String project) { _project = project }
    void tempLocation(final String location) { _tempLocation = location }
    void publishToBigQuery(final boolean publish) { _publishToBigQuery = publish }
    void metricsDataset(final String dataset) { _metricsDataset = dataset }
    void metricsTable(final String table) { _metricsTable = table }
    void inputOptions(final InputOptions options) { _inputOptions = options }
    void numWorkers(final int workers) { _numWorkers = workers }
    void autoscalingAlgorithm(final String algorithm) { _autoscalingAlgorithm = algorithm }
    void region(final String region) { _region = region }
    void jobEndpoint(final String endpoint) { _jobEndpoint = endpoint }
    void environmentType(final String type) { _environmentType = type }
    void environmentConfig(final String config) { _environmentConfig = config }
    void parallelism(final int parallelism) { _parallelism = parallelism }
    void bigQueryDataset(final String dataset) { _bigQueryDataset = dataset }
    void bigQueryTable(final String table) { _bigQueryTable = table }
    void streaming(final boolean isStreaming) { _streaming = isStreaming }
    void sourceOptions(final Closure cl = {}) { _sourceOptions = makeSourceOptions(cl) }
    void coSourceOptions(final Closure cl = {}) { _coSourceOptions = makeSourceOptions(cl) }
    void inputOptions(final Closure cl = {}) { _inputOptions = makeInputOptions(cl) }
    void coInputOptions(final Closure cl = {}) { _coInputOptions = makeInputOptions(cl) }
    void stepOptions(final Closure cl = {}) { _stepOptions = makeStepOptions(cl) }
    void specificParameters(final Map<String, Object> map) { _specificParameters.putAll(map) }

    //sdk -- snake_case vs camelCase
    void python() { i_sdk = SDK.PYTHON }
    void python37() { i_sdk = SDK.PYTHON_37 }
    void java() { i_sdk = SDK.JAVA }


    private InputOptions makeInputOptions(final Closure cl = {}) {
      return makeOptions(cl, _inputOptions ?: InputOptions.withSDK(i_sdk))
    }

    private SourceOptions makeSourceOptions(final Closure cl = {}) {
      return makeOptions(cl, _sourceOptions ?: SourceOptions.withSDK(i_sdk))
    }

    private StepOptions makeStepOptions(final Closure cl = {}) {
      return makeOptions(cl, _stepOptions ?: StepOptions.withSDK(i_sdk))
    }

    private <T> T makeOptions(final Closure cl = {}, final T options) {
      final def code = cl.rehydrate(options, this, this)
      code.resolveStrategy = Closure.DELEGATE_ONLY
      code()
      return options
    }

    @Override
    Map<String, Serializable> toPrimitiveValues() {
      final def map = propertiesMap
      verifyPipelineProperties(map)
      return ConfigHelper.convertProperties(map, i_sdk)
    }

    private void verifyPipelineProperties(final Map<String, Object> map) {
      verifyRequired(map)
      switch (i_runner) {
        case Runner.DATAFLOW:
          verifyDataflowProperties(map)
          break
        case Runner.PORTABLE:
          verifyPortableProperties(map)
          break
        default:
          break
      }
    }

    private void verifyRequired(final Map<String, Object> map) {
      verifyCommonRequired(map)
      switch (i_sdk) {
        case SDK.PYTHON:
        case SDK.PYTHON_37:
          verifyPythonRequired(map)
          break
        case SDK.JAVA:
          verifyJavaRequired(map)
          break
        default:
          break
      }
    }

    private static void verifyCommonRequired(final Map<String, Object> map) {
      verify(map, "") { i_required.contains(it.key) }
    }

    private static void verifyPythonRequired(final Map<String, Object> map) {
      verify(map, "for Python SDK") { i_pythonRequired.contains(it.key) }
    }

    private static void verifyJavaRequired(final Map<String, Object> map) {
      verify(map, "for Java SDK") { i_javaRequired.contains(it.key) }
    }

    private static void verifyDataflowProperties(final Map<String, Object> map) {
      verify(map, "for Dataflow runner") { i_dataflowRequired.contains(it.key) }
    }

    private static void verifyPortableProperties(final Map<String, Object> map) {
      verify(map, "for Portable runner") { i_portableRequired.contains(it.key) }
    }

    private static void verify(final Map<String, Object> map, final String message, final Predicate<Map.Entry<String, Object>> predicate) {
      map.entrySet()
          .stream()
          .filter(predicate)
          .forEach{ requireNonNull(it.value, "${it.key.substring(1)} is required " + message) }
    }

    static PipelineOptions of(final PipelineOptions options) {
      final def newOptions = new PipelineOptions()

      //primitive values
      InvokerHelper.setProperties(newOptions, options.propertiesMap)

      //non-primitive
      newOptions._inputOptions = options._inputOptions ? InputOptions.of(options._inputOptions) : null
      newOptions._coInputOptions = options._coInputOptions ? InputOptions.of(options._coInputOptions) : null
      newOptions._sourceOptions = options._sourceOptions ? SourceOptions.of(options._sourceOptions) : null
      newOptions._coSourceOptions = options._coSourceOptions ? SourceOptions.of(options._coSourceOptions) : null
      newOptions._stepOptions = options._stepOptions ? StepOptions.of(options._stepOptions) : null
      newOptions._specificParameters = new HashMap<>(options._specificParameters)

      return newOptions
    }

    Map<String, Object> getPropertiesMap() {
      return [
        i_sdk: i_sdk,
        i_runner: i_runner,
        _jobName: _jobName,
        _appName: _appName,
        _project: _project,
        _tempLocation: _tempLocation,
        _publishToBigQuery: _publishToBigQuery,
        _metricsDataset: _metricsDataset,
        _metricsTable: _metricsTable,
        _numWorkers: _numWorkers,
        _autoscalingAlgorithm: _autoscalingAlgorithm,
        _region: _region,
        _inputOptions: _inputOptions,
        _coInputOptions: _coInputOptions,
        _jobEndpoint: _jobEndpoint,
        _environmentType: _environmentType,
        _environmentConfig: _environmentConfig,
        _parallelism: _parallelism,
        _bigQueryDataset: _bigQueryDataset,
        _bigQueryTable: _bigQueryTable,
        _streaming: _streaming,
        _sourceOptions: _sourceOptions,
        _coSourceOptions: _coSourceOptions,
        _stepOptions: _stepOptions
      ].putAll(_specificParameters.entrySet())
    }

    private static class InputOptions implements SerializableOption<String> {
      private def _numRecords
      private def _keySize
      private def _valueSize
      private def _numHotKeys
      private def _hotKeyFraction

      //internal usage
      private SDK i_sdk

      private InputOptions() {}

      static withSDK(final SDK sdk) {
        final def input = new InputOptions()
        input.i_sdk = sdk
        return input
      }

      void numRecords(final int num) { _numRecords = num }
      void keySize(final int size) { _keySize = size }
      void valueSize(final int size) { _valueSize = size }
      void numHotsKeys(final int num) { _numHotKeys = num }
      void hotKeyFraction(final int fraction) { _hotKeyFraction = fraction }

      @Override
      String toPrimitiveValues() {
        return "'${new JsonBuilder(ConfigHelper.convertProperties(propertiesMap, i_sdk)).toString()}'"
      }

      static InputOptions of(final InputOptions oldOptions) {
        final def newOptions = new InputOptions()
        InvokerHelper.setProperties(newOptions, oldOptions.propertiesMap)
        return newOptions
      }

      LinkedHashMap<String, Object> getPropertiesMap() {
        return [
          i_sdk: i_sdk,
          _numRecords: _numRecords,
          _keySize: _keySize,
          _valueSize: _valueSize,
          _numHotKeys: _numHotKeys,
          _hotKeyFraction: _hotKeyFraction
        ] as LinkedHashMap<String, Object>
      }
    }

    private static class SourceOptions implements SerializableOption<String> {
      private def _numRecords
      private def _keySizeBytes
      private def _valueSizeBytes
      private def _numHotKeys
      private def _hotKeyFraction
      private def _splitPointFrequencyRecords

      //internal usage
      private SDK i_sdk

      private SourceOptions() {}

      static withSDK(final SDK sdk) {
        final def input = new SourceOptions()
        input.i_sdk = sdk
        return input
      }

      void numRecords(final int num) { _numRecords = num }
      void keySizeBytes(final int size) { _keySizeBytes = size }
      void valueSizeBytes(final int size) { _valueSizeBytes = size }
      void numHotsKeys(final int num) { _numHotKeys = num }
      void hotKeyFraction(final int fraction) { _hotKeyFraction = fraction }
      void splitPointFrequencyRecords(final int splitPoint) { _splitPointFrequencyRecords = splitPoint }

      @Override
      String toPrimitiveValues() {
        return new JsonBuilder(ConfigHelper.convertProperties(propertiesMap, i_sdk)).toString()
      }

      static SourceOptions of(final SourceOptions oldOptions) {
        final def newOptions = new SourceOptions()
        InvokerHelper.setProperties(newOptions, oldOptions.propertiesMap)
        return newOptions
      }

      Map<String, Object> getPropertiesMap() {
        return [
          i_sdk: i_sdk,
          _numRecords: _numRecords,
          _keySizeBytes: _keySizeBytes,
          _valueSizeBytes: _valueSizeBytes,
          _numHotKeys: _numHotKeys,
          _hotKeyFraction: _hotKeyFraction,
          _splitPointFrequencyRecords: _splitPointFrequencyRecords
        ]
      }
    }

    private static class StepOptions implements SerializableOption<String> {
      private def _outputRecordsPerInputRecord
      private boolean  _preservesInputKeyDistribution

      //internal usage
      private SDK i_sdk

      private StepOptions() {}

      static withSDK(final SDK sdk) {
        final def option = new StepOptions()
        option.i_sdk = sdk
        return option
      }

      void outputRecordsPerInputRecord(final int records) { _outputRecordsPerInputRecord = records }
      void preservesInputKeyDistribution(final boolean  shouldPreserve) { _preservesInputKeyDistribution = shouldPreserve }

      @Override
      String toPrimitiveValues() {
        return new JsonBuilder(ConfigHelper.convertProperties(propertiesMap, i_sdk)).toString()
      }

      Map<String, Object> getPropertiesMap() {
        return [
          i_sdk: i_sdk,
          _outputRecordsPerInputRecord: _outputRecordsPerInputRecord,
          _preservesInputKeyDistribution: _preservesInputKeyDistribution
        ] as Map<String, Object>
      }

      static StepOptions of(final StepOptions oldOption) {
        final def newOption = new StepOptions()
        InvokerHelper.setProperties(newOption, oldOption.propertiesMap)
        return newOption
      }
    }
  }

  private interface SerializableOption<T> {
    T toPrimitiveValues()
  }

  private static class ConfigHelper {
    private static final List<String> FIELDS_TO_REMOVE = ["class", "i_sdk", "i_runner"]

    static Map<String, Serializable> convertProperties(final Map<String, Object> propertyMap, final SDK sdk = SDK.JAVA) {
      return propertyMap
          .findAll { nonNull(it.value) }
          .findAll { !FIELDS_TO_REMOVE.contains(it.key) }
          .collectEntries { key, value ->
            [
              modifyKey(key, sdk),
              toPrimitive(value)
            ]
          } as Map<String, Serializable>
    }

    private static String modifyKey(final String key, final SDK sdk) {
      final def result = key.startsWith('_') ? key.substring(1) : key
      switch (sdk) {
        case SDK.PYTHON_37:
        case SDK.PYTHON:
          return toSnakeCase(result)
        case SDK.JAVA:
          return toCamelCase(result)
        default:
          throw new IllegalArgumentException("SDK not specified")
      }
    }

    private static String toSnakeCase(final String text) {
      return text.replaceAll(/([A-Z])/, /_$1/).toLowerCase().replaceAll(/^_/, '')
    }

    private static String toCamelCase(final String text) {
      return text.replaceAll( "(_)([A-Za-z0-9])", { Object[] it -> ((String) it[2]).toUpperCase() })
    }

    private static def toPrimitive(value) {
      return value instanceof SerializableOption
          ? value.toPrimitiveValues()
          : value
    }
  }
}
