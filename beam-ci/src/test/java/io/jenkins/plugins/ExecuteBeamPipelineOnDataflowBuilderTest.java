package io.jenkins.plugins;

import hudson.model.FreeStyleBuild;
import hudson.model.FreeStyleProject;
import hudson.model.Result;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;


public class ExecuteBeamPipelineOnDataflowBuilderTest {

    @Rule
    public JenkinsRule jenkins = new JenkinsRule();

    @Mock
    ProcessBuilder processBuilderMock;

    final String pathToCreds = "path/to/credentials";
    final String pathToMainClass = "path/to/main/class";
    final String pipelineOptions = "fake pipeline options";
    final String buildReleaseOptions = "-Pdataflow-runner";

    final ArrayList<String> expectedGradleCommand = new ArrayList<>(Arrays.asList("gradle", "clean", "execute", "-DmainClass=" + pathToMainClass, "-Dexec.args=" + pipelineOptions, buildReleaseOptions));
    final ArrayList<String> expectedMavenCommand = new ArrayList<>(Arrays.asList("mvn", "compile", "exec:java", "-Dexec.mainClass=" + pathToMainClass, "-Dexec.args=" + pipelineOptions, buildReleaseOptions));

    @Before
    public void initMocks() {
        processBuilderMock = Mockito.mock(ProcessBuilder.class);
        MockitoAnnotations.initMocks(this);
        Map<String,String> environment = new HashMap<>();
        when(processBuilderMock.environment()).thenReturn(environment);
        when(processBuilderMock.directory(any())).thenReturn(null);
        when(processBuilderMock.command(anyList())).thenReturn(null);
    }

    @Test
    public void testGradleCommandConfiguration() throws Exception {
        generalTestJavaCommand(expectedGradleCommand, true);
    }

    @Test
    public void testMavenCommandConfiguration() throws Exception {
        generalTestJavaCommand(expectedMavenCommand, false);
    }

    @Test
    public void testPythonCommandConfiguration() throws Exception {
        // Check for both cases of useGradle being true/false
        FreeStyleProject project = jenkins.createFreeStyleProject();
        boolean useJava = false;
        boolean useGradle = false;

        ExecuteBeamPipelineOnDataflowBuilder builder = new ExecuteBeamPipelineOnDataflowBuilder(pathToCreds, pathToMainClass, pipelineOptions, buildReleaseOptions, useJava, useGradle);
        builder.setProcessBuilder(processBuilderMock);

        project.getBuildersList().add(builder);
        FreeStyleBuild build = jenkins.buildAndAssertSuccess(project);

        ArrayList<String> command = builder.getCommand();
        assertEquals(6, command.size());
        assertThat(command.get(0), containsString("executePythonBeamPipeline.sh"));
        assertEquals(pathToMainClass, command.get(3));
        assertEquals(pipelineOptions, command.get(4));
        assertEquals(buildReleaseOptions, command.get(5));
        jenkins.assertLogContains("Test finished successfully.", build);
    }

    /**
    * Test generating the correct command for running a Beam Pipeline in either Maven or Gradle
    * */
    private void generalTestJavaCommand(ArrayList<String> expectedCommand, boolean useGradle) throws Exception {
        FreeStyleProject project = jenkins.createFreeStyleProject();

        ExecuteBeamPipelineOnDataflowBuilder builder = new ExecuteBeamPipelineOnDataflowBuilder(pathToCreds, pathToMainClass, pipelineOptions, buildReleaseOptions, true, useGradle);
        builder.setProcessBuilder(processBuilderMock);

        project.getBuildersList().add(builder);
        FreeStyleBuild build = jenkins.buildAndAssertSuccess(project);
        assertEquals(expectedCommand, builder.getCommand());
        jenkins.assertLogContains("Test finished successfully.", build);
    }


}
