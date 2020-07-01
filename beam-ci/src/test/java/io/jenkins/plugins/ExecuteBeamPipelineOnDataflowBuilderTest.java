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
package io.jenkins.plugins;

import hudson.model.FreeStyleBuild;
import hudson.model.FreeStyleProject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.mockito.*;

import java.io.*;
import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class ExecuteBeamPipelineOnDataflowBuilderTest {

    @Rule
    public JenkinsRule jenkins = new JenkinsRule();

    @Mock
    PipelineLauncher pipelineLauncherMock;

    @Mock
    PipelineLauncher.LaunchProcess launchProcessMock;

    final String pathToCreds = "path/to/credentials";
    final String pathToMainClass = "path/to/main/class";
    final String pipelineOptions = "fake pipeline options";
    final String buildReleaseOptions = "-Pdataflow-runner";

    final ArrayList<String> expectedGradleCommand = new ArrayList<>(Arrays.asList("gradle", "clean", "execute", "-DmainClass=" + pathToMainClass, "-Dexec.args=" + pipelineOptions, buildReleaseOptions));
    final ArrayList<String> expectedMavenCommand = new ArrayList<>(Arrays.asList("mvn", "compile", "exec:java", "-Dexec.mainClass=" + pathToMainClass, "-Dexec.args=" + pipelineOptions, buildReleaseOptions));

    @Before
    public void initMocks() throws InterruptedException, IOException {
        pipelineLauncherMock = Mockito.mock(PipelineLauncher.class);
        launchProcessMock = Mockito.mock(PipelineLauncher.LaunchProcess.class);
        MockitoAnnotations.initMocks(this);
        InputStream is = new ByteArrayInputStream( "".getBytes() );
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
        when(launchProcessMock.getInputStream()).thenReturn(bufferedReader);
        when(launchProcessMock.getErrorStream()).thenReturn(bufferedReader);
        when(launchProcessMock.waitFor()).thenReturn(0);
        when(pipelineLauncherMock.start()).thenReturn(launchProcessMock);
    }

    @Test
    public void testGradleCommandConfiguration() throws Exception {
        generalTestJavaCommand(expectedGradleCommand, true);
    }

    @Test
    public void testMavenCommandConfiguration() throws Exception {
        generalTestJavaCommand(expectedMavenCommand, false);
    }

    /**
     * Test generating the correct command for running a Beam Pipeline in Python
     * */
    @Test
    public void testPythonCommandConfiguration() throws Exception {
        FreeStyleProject project = jenkins.createFreeStyleProject();
        boolean useGradle = false;

        ExecuteBeamPipelineOnDataflowBuilder builder = new ExecuteBeamPipelineOnDataflowBuilder(pathToCreds, pathToMainClass, pipelineOptions, buildReleaseOptions, "Python", useGradle);
        builder.setPipelineLauncher(pipelineLauncherMock);

        project.getBuildersList().add(builder);
        FreeStyleBuild build = jenkins.buildAndAssertSuccess(project);

        ArrayList<String> command = builder.getCommand();
        assertEquals(6, command.size());
        assertThat(command.get(0), containsString("executePythonBeamPipeline.sh"));
        assertEquals(pathToMainClass, command.get(3));
        assertEquals(pipelineOptions, command.get(4));
        assertEquals(buildReleaseOptions, command.get(5));
        jenkins.assertLogContains("Exited with error code : 0", build);
        jenkins.assertLogContains("Finished: SUCCESS", build);
    }

    /**
    * Test generating the correct command for running a Beam Pipeline in either Maven or Gradle
    * */
    private void generalTestJavaCommand(ArrayList<String> expectedCommand, boolean useGradle) throws Exception {
        FreeStyleProject project = jenkins.createFreeStyleProject();

        ExecuteBeamPipelineOnDataflowBuilder builder = new ExecuteBeamPipelineOnDataflowBuilder(pathToCreds, pathToMainClass, pipelineOptions, buildReleaseOptions, "Java", useGradle);
        builder.setPipelineLauncher(pipelineLauncherMock);

        project.getBuildersList().add(builder);
        FreeStyleBuild build = jenkins.buildAndAssertSuccess(project);
        assertEquals(expectedCommand, builder.getCommand());
        jenkins.assertLogContains("Exited with error code : 0", build);
        jenkins.assertLogContains("Finished: SUCCESS", build);
    }


}
