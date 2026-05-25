package com.gradle.gradlebuildaction

import org.gradle.util.GradleVersion

import static org.junit.Assume.assumeTrue

class TestDependencyGraph extends BaseInitScriptTest {
    def initScript = 'gradle-actions.github-dependency-graph.init.gradle'

    static final TestGradleVersion GRADLE_5_1 = new TestGradleVersion(GradleVersion.version('5.1.1'), 8, 12)
    static final TestGradleVersion GRADLE_7_0 = new TestGradleVersion(GradleVersion.version('7.0.1'), 8, 12)

    static final List<TestGradleVersion> NO_DEPENDENCY_GRAPH_VERSIONS = [GRADLE_3_X, GRADLE_4_X, GRADLE_5_1, GRADLE_7_0]
    static final List<TestGradleVersion> DEPENDENCY_GRAPH_VERSIONS = ALL_VERSIONS - NO_DEPENDENCY_GRAPH_VERSIONS

    def "does not produce dependency graph when not enabled"() {
        assumeTrue testGradleVersion.compatibleWithCurrentJvm

        when:
        run(['help'], initScript, testGradleVersion.gradleVersion)

        then:
        assert !reportsDir.exists()

        where:
        testGradleVersion << ALL_VERSIONS
    }

    def "produces dependency graph when enabled"() {
        assumeTrue testGradleVersion.compatibleWithCurrentJvm

        when:
        run(['help'], initScript, testGradleVersion.gradleVersion, jvmArgs, envVars)

        then:
        assert reportFile.exists()

        where:
        testGradleVersion << DEPENDENCY_GRAPH_VERSIONS
    }

    def "produces dependency graph with configuration-cache on latest Gradle"() {
        assumeTrue testGradleVersion.compatibleWithCurrentJvm

        when:
        run(['help', '--configuration-cache'], initScript, testGradleVersion.gradleVersion, jvmArgs, envVars)

        then:
        assert reportFile.exists()

        where:
        // Dependency-graph plugin doesn't support config-cache for 8.0 of Gradle
        testGradleVersion << [GRADLE_8_X]
    }

    def "warns and produces no dependency graph when enabled for older Gradle versions"() {
        assumeTrue testGradleVersion.compatibleWithCurrentJvm

        when:
        def result = run(['help'], initScript, testGradleVersion.gradleVersion, jvmArgs, envVars)

        then:
        assert !reportsDir.exists()
        assert result.output.contains("::warning::Dependency Graph is not supported for ${testGradleVersion}")

        where:
        testGradleVersion << NO_DEPENDENCY_GRAPH_VERSIONS
    }

    def "fails build when enabled for older Gradle versions if continue-on-failure is false"() {
        assumeTrue testGradleVersion.compatibleWithCurrentJvm

        when:
        def vars = envVars
        vars.put('GITHUB_DEPENDENCY_GRAPH_CONTINUE_ON_FAILURE', 'false')
        def args = jvmArgs
        Collections.replaceAll(args,
            '-DGITHUB_DEPENDENCY_GRAPH_CONTINUE_ON_FAILURE=true',
            '-DGITHUB_DEPENDENCY_GRAPH_CONTINUE_ON_FAILURE=false'
        )
        def result = runAndFail(['help'], initScript, testGradleVersion.gradleVersion, args, vars)

        then:
        assert !reportsDir.exists()
        assert result.output.contains("Dependency Graph is not supported for ${testGradleVersion}")

        where:
        testGradleVersion << NO_DEPENDENCY_GRAPH_VERSIONS
    }

    def "constructs unique job correlator for each build invocation"() {
        assumeTrue testGradleVersion.compatibleWithCurrentJvm

        def reportFile1 = new File(reportsDir, "CORRELATOR-1.json")
        def reportFile2 = new File(reportsDir, "CORRELATOR-2.json")

        buildFile << """
            task firstTask {
                doLast {
                    println "First"
                }
            }
            task secondTask {
                doLast {
                    println "Second"
                }
            }
        """

        when:
        run(['help'], initScript, testGradleVersion.gradleVersion, jvmArgs, envVars)

        then:
        assert reportFile.exists()

        when:
        run(['first'], initScript, testGradleVersion.gradleVersion, jvmArgs, envVars)

        then:
        assert reportFile.exists()
        assert reportFile1.exists()
        
        when:
        run(['second'], initScript, testGradleVersion.gradleVersion, jvmArgs, envVars)

        then:
        assert reportFile.exists()
        assert reportFile1.exists()
        assert reportFile2.exists()
        
        where:
        testGradleVersion << DEPENDENCY_GRAPH_VERSIONS
    }

    def "can configure alternative repository for plugins"() {
        assumeTrue testGradleVersion.compatibleWithCurrentJvm

        when:
        def vars = envVars
        vars.put('GRADLE_PLUGIN_REPOSITORY_URL', 'https://plugins.grdev.net/m2')
        // TODO:DAZ This props are set too late to control init-script plugin resolution
        // This makes the tests fail on Mac with Gradle < 6
        def args = jvmArgs
        args.add('-Dgradle.plugin-repository.url=https://plugins.grdev.net/m2')
        def result = run(['help', '--info'], initScript, testGradleVersion.gradleVersion, args, vars)

        then:
        assert reportFile.exists()
        assert result.output.find("Resolving dependency graph plugin [\\d\\.]+ from plugin repository: https://plugins.grdev.net/m2")

        where:
        testGradleVersion << DEPENDENCY_GRAPH_VERSIONS
    }

    def "can provide credentials for alternative repository for plugins"() {
        assumeTrue testGradleVersion.compatibleWithCurrentJvm

        when:
        def vars = envVars
        vars.put('GRADLE_PLUGIN_REPOSITORY_URL', 'https://plugins.grdev.net/m2')
        vars.put('GRADLE_PLUGIN_REPOSITORY_USERNAME', 'REPO_USER')
        vars.put('GRADLE_PLUGIN_REPOSITORY_PASSWORD', 'REPO_PASSWORD')

        def args = jvmArgs
        args.add('-Dgradle.plugin-repository.url=https://plugins.grdev.net/m2')
        args.add('-Dgradle.plugin-repository.username=REPO_USER')
        args.add('-Dgradle.plugin-repository.password=REPO_PASSWORD')
        def result = run(['help', '--info'], initScript, testGradleVersion.gradleVersion, args, vars)

        then:
        assert reportFile.exists()
        assert result.output.find("Resolving dependency graph plugin [\\d\\.]+ from plugin repository: https://plugins.grdev.net/m2")
        assert result.output.contains("Applying credentials for plugin repository: https://plugins.grdev.net/m2")

        where:
        testGradleVersion << DEPENDENCY_GRAPH_VERSIONS
    }

    def getEnvVars() {
        return [
            GITHUB_DEPENDENCY_GRAPH_ENABLED: "true",
            GITHUB_DEPENDENCY_GRAPH_CONTINUE_ON_FAILURE: "true",
            GITHUB_DEPENDENCY_GRAPH_JOB_CORRELATOR: "CORRELATOR",
            GITHUB_DEPENDENCY_GRAPH_JOB_ID: "1",
            GITHUB_DEPENDENCY_GRAPH_REF: "main",
            GITHUB_DEPENDENCY_GRAPH_SHA: "123456",
            GITHUB_DEPENDENCY_GRAPH_WORKSPACE: testProjectDir.absolutePath,
            DEPENDENCY_GRAPH_REPORT_DIR: reportsDir.absolutePath,
        ]
    }

    def getJvmArgs() {
        return [
            "-DGITHUB_DEPENDENCY_GRAPH_ENABLED=true",
            "-DGITHUB_DEPENDENCY_GRAPH_CONTINUE_ON_FAILURE=true",
            "-DGITHUB_DEPENDENCY_GRAPH_JOB_CORRELATOR=CORRELATOR",
            "-DGITHUB_DEPENDENCY_GRAPH_JOB_ID=1",
            "-DGITHUB_DEPENDENCY_GRAPH_REF=main",
            "-DGITHUB_DEPENDENCY_GRAPH_SHA=123456",
            "-DGITHUB_DEPENDENCY_GRAPH_WORKSPACE=" + testProjectDir.absolutePath,
            "-DDEPENDENCY_GRAPH_REPORT_DIR=" + reportsDir.absolutePath,
        ].collect {it.toString() } // Convert from GString to String
    }

    def getReportsDir() {
        return new File(testProjectDir, 'build/reports/github-dependency-graph-snapshots')
    }

    def getReportFile() {
        return new File(reportsDir, "CORRELATOR.json")
    }
}
