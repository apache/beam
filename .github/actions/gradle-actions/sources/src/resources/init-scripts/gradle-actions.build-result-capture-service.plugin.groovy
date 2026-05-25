import org.gradle.tooling.events.*
import org.gradle.tooling.events.task.*
import org.gradle.internal.operations.*
import org.gradle.initialization.*
import org.gradle.api.internal.tasks.execution.*
import org.gradle.execution.*
import org.gradle.internal.build.event.BuildEventListenerRegistryInternal
import org.gradle.util.GradleVersion
import org.slf4j.LoggerFactory

settingsEvaluated { settings ->
    def projectTracker = gradle.sharedServices.registerIfAbsent("gradle-action-buildResultsRecorder", BuildResultsRecorder, { spec ->
        spec.getParameters().getRootProjectName().set(settings.rootProject.name)
        spec.getParameters().getRootProjectDir().set(settings.rootDir.absolutePath)
        spec.getParameters().getRequestedTasks().set(gradle.startParameter.taskNames.join(" "))
        spec.getParameters().getGradleHomeDir().set(gradle.gradleHomeDir.absolutePath)
        spec.getParameters().getInvocationId().set(gradle.ext.invocationId)
    })

    gradle.services.get(BuildEventListenerRegistryInternal).onOperationCompletion(projectTracker)
}

abstract class BuildResultsRecorder implements BuildService<BuildResultsRecorder.Params>, BuildOperationListener, AutoCloseable {
    private final logger = LoggerFactory.getLogger("gradle/actions")
    private boolean buildFailed = false
    private boolean configCacheHit = true
    interface Params extends BuildServiceParameters {
        Property<String> getRootProjectName()
        Property<String> getRootProjectDir()
        Property<String> getRequestedTasks()
        Property<String> getGradleHomeDir()
        Property<String> getInvocationId()
    }

    void started(BuildOperationDescriptor buildOperation, OperationStartEvent startEvent) {}

    void progress(OperationIdentifier operationIdentifier, OperationProgressEvent progressEvent) {}

    void finished(BuildOperationDescriptor buildOperation, OperationFinishEvent finishEvent) {
        if (buildOperation.details in EvaluateSettingsBuildOperationType.Details) {
            // Got EVALUATE SETTINGS event: not a config-cache hit"
            configCacheHit = false
        }
        if (buildOperation.metadata == BuildOperationCategory.RUN_WORK ||
            buildOperation.metadata == BuildOperationCategory.CONFIGURE_PROJECT) {
            if (finishEvent.failure != null) {
                buildFailed = true
            }
        }
    }

    @Override
    public void close() {
        def buildResults = [
            rootProjectName: getParameters().getRootProjectName().get(),
            rootProjectDir: getParameters().getRootProjectDir().get(),
            requestedTasks: getParameters().getRequestedTasks().get(),
            gradleVersion: GradleVersion.current().version,
            gradleHomeDir: getParameters().getGradleHomeDir().get(),
            buildFailed: buildFailed,
            configCacheHit: configCacheHit
        ]

        def runnerTempDir = System.getProperty("RUNNER_TEMP") ?: System.getenv("RUNNER_TEMP")
        def githubActionStep = System.getProperty("GITHUB_ACTION") ?: System.getenv("GITHUB_ACTION")
        if (!runnerTempDir || !githubActionStep) {
            return
        }

        try {
            def buildResultsDir = new File(runnerTempDir, ".gradle-actions/build-results")
            buildResultsDir.mkdirs()
            def buildResultsFile = new File(buildResultsDir, githubActionStep + getParameters().getInvocationId().get() + ".json")
            if (!buildResultsFile.exists()) {
                logger.lifecycle("gradle/actions: Writing build results to ${buildResultsFile}")
                buildResultsFile << groovy.json.JsonOutput.toJson(buildResults)
            }
        } catch (Exception e) {
            println "\ngradle action failed to write build-results file. Will continue.\n> ${e.getLocalizedMessage()}"
        }
    }
}
