Addresses #36790: "[Feature Request]: Make lineage tracking pluggable"

## Changes

- **Created** `org.apache.beam.sdk.lineage.LineageBase` - Plugin interface with single `add()` method
- **Refactored** `org.apache.beam.sdk.metrics.Lineage` - Hardcoded metrics → Delegation to `LineageBase` plugins
- **Created** `org.apache.beam.sdk.lineage.LineageRegistrar` - Plugin discovery interface (ServiceLoader)
- **Extracted** `org.apache.beam.sdk.metrics.MetricsLineage` - Default metrics-based implementation (implements `LineageBase`)
- **Added** Plugin initialization in `FileSystems.setDefaultPipelineOptions()`

## Architecture

**Before (master)**: `Lineage` was a concrete class hardcoded to use Beam metrics:

```java
public class Lineage {
  private static final Lineage SOURCES = new Lineage(Type.SOURCE);
  private static final Lineage SINKS = new Lineage(Type.SINK);
  private final Metric metric;  // Hardcoded to Beam metrics

  private Lineage(Type type) {
    this.metric = Metrics.stringSet(LINEAGE_NAMESPACE, type.toString());
  }

  public void add(Iterable<String> segments) {
    ((StringSet) metric).add(String.join("", segments));  // Always metrics
  }
}
```

**After (this PR)**: Clean separation via composition pattern:

```java
// Plugin contract (simple interface)
public interface LineageBase {
  void add(Iterable<String> rollupSegments);
}

// Public API (final facade delegating to plugin)
public final class Lineage {
  private final LineageBase delegate;  // Plugin implementation

  private Lineage(LineageBase delegate) {
    this.delegate = delegate;
  }

  // Delegates to plugin
  public void add(Iterable<String> segments) {
    delegate.add(segments);
  }

  // Convenience overloads
  public void add(String system, Iterable<String> segments) { ... }
  public void add(String system, String subtype, ...) { ... }

  // Static utilities (unchanged)
  public static Lineage getSources() { ... }
  public static Lineage getSinks() { ... }
  public static String wrapSegment(String value) { ... }
  public static Set<String> query(MetricResults results, Type type) { ... }
}

// Default implementation (backward compatible)
public class MetricsLineage implements LineageBase {
  private final Metric metric;

  @Override
  public void add(Iterable<String> segments) {
    ((BoundedTrie) metric).add(segments);
  }
}
```

**Plugin Selection**: ServiceLoader discovery, first match wins, fallback to `MetricsLineage`.

**Backward Compatibility**: ✅ All existing code works unchanged (24+ call sites, static utilities, enums).

## Why Pluggable Lineage?

### 1. Runner Fragmentation

Metrics-based lineage is **scattered across runners** with inconsistent support:
- **Dataflow**: Real-time metrics export to Cloud Monitoring
- **Flink**: Batch-only aggregation (no streaming support yet)
- **Spark/Direct**: Varying levels of support

**Impact**: Multi-runner organizations must consolidate lineage from different metrics backends, each with different APIs and formats.

**Plugin Solution**: Single implementation works consistently across all runners.

### 2. Enterprise Integration

Organizations with existing lineage infrastructure need:
- **Direct API integration** (Atlan, Collibra, Marquez, DataHub, OpenLineage)
- **Custom metadata enrichment** not in metrics subsystem

**Example**: Flyte workflow executing a Beam pipeline needs to tag lineage with Flyte execution ID and cost allocation. This context exists in the orchestrator, not in Beam workers' metrics.

### 3. Standard Formats

OpenLineage is the industry standard. Plugin enables direct emission vs. export metrics → parse → transform → send.

## Initialization

`Lineage.setDefaultPipelineOptions(options)` is called from `FileSystems.setDefaultPipelineOptions()` (same pattern as `Metrics`).

**Rationale**: `FileSystems.setDefaultPipelineOptions()` is called at 48+ locations covering all execution scenarios (pipeline construction, worker startup, deserialization).

**Known Limitation**: Follows existing `FileSystems` pattern despite known issues ([#18430](https://github.com/apache/beam/issues/18430)). Architectural improvements would address all subsystems together.

## Thread Safety

Uses `AtomicReference` with `compareAndSet` loop (same pattern as `FileSystems`/`Metrics`):
- `AtomicReference<KV<Long, Integer>>` tracks PipelineOptions identity
- `AtomicReference<Lineage>` for SOURCES/SINKS instances

## Example: OpenLineage Plugin

_For demonstration only (OpenLineage integration out of scope)_

```java
// 1. Plugin options
public interface OpenLineageOptions extends PipelineOptions {
  @Description("OpenLineage endpoint URL")
  String getOpenLineageUrl();
  void setOpenLineageUrl(String url);

  @Description("Enable OpenLineage plugin")
  @Default.Boolean(false)
  Boolean getEnableOpenLineage();
  void setEnableOpenLineage(Boolean enable);
}

// 2. Implement LineageBase
class OpenLineageReporter implements LineageBase {
  private final String endpoint;
  private final Lineage.LineageDirection direction;

  @Override
  public void add(Iterable<String> rollupSegments) {
    String fqn = String.join("", rollupSegments);
    // POST to OpenLineage API with workflow context
    sendToOpenLineage(endpoint, direction, fqn);
  }
}

// 3. Register via ServiceLoader
@AutoService(LineageRegistrar.class)
public class OpenLineageRegistrar implements LineageRegistrar {
  @Override
  public LineageBase fromOptions(PipelineOptions options, Lineage.LineageDirection direction) {
    OpenLineageOptions opts = options.as(OpenLineageOptions.class);
    if (opts.getEnableOpenLineage()) {
      return new OpenLineageReporter(opts.getOpenLineageUrl(), direction);
    }
    return null; // Fall back to MetricsLineage
  }
}

// 4. Usage
PipelineOptions options = PipelineOptionsFactory.create();
options.as(OpenLineageOptions.class).setEnableOpenLineage(true);
options.as(OpenLineageOptions.class).setOpenLineageUrl("https://lineage-api.example.com");
Pipeline p = Pipeline.create(options);
```