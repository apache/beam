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
package org.apache.beam.sdk.io.hadoop.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests functionality of {@link HDFSSynchronization} class. */
@RunWith(JUnit4.class)
public class HDFSSynchronizationTest {
  private static final String DEFAULT_JOB_ID = String.valueOf(1);
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private HDFSSynchronization tested;
  private Configuration configuration;

  @Before
  public void setup() {
    this.tested = new HDFSSynchronization(tmpFolder.getRoot().getAbsolutePath());
    this.configuration = new Configuration();
    configuration.set(HadoopFormatIO.JOB_ID, DEFAULT_JOB_ID);
  }

  /** Tests that job lock will be acquired only once until it is again released. */
  @Test
  public void tryAcquireJobLockTest() {
    boolean firstAttempt = tested.tryAcquireJobLock(configuration);
    boolean secondAttempt = tested.tryAcquireJobLock(configuration);
    boolean thirdAttempt = tested.tryAcquireJobLock(configuration);

    assertTrue(isFileExists(getJobLockPath()));

    tested.releaseJobIdLock(configuration);

    boolean fourthAttempt = tested.tryAcquireJobLock(configuration);
    boolean fifthAttempt = tested.tryAcquireJobLock(configuration);

    assertTrue(firstAttempt);
    assertFalse(secondAttempt);
    assertFalse(thirdAttempt);

    assertTrue(fourthAttempt);
    assertFalse(fifthAttempt);
  }

  /** Missing job id in configuration will throw exception. */
  @Test(expected = NullPointerException.class)
  public void testMissingJobId() {
    Configuration conf = new Configuration();
    tested.tryAcquireJobLock(conf);
  }

  /** Multiple attempts to release job will not throw exception. */
  @Test
  public void testMultipleTaskDeletion() {
    String jobFolder = getFileInJobFolder("");

    tested.tryAcquireJobLock(configuration);

    assertTrue(isFileExists(getJobLockPath()));

    tested.releaseJobIdLock(configuration);

    assertFalse(isFileExists(getJobLockPath()));
    assertFalse(isFolderExists(jobFolder));

    // any exception will not be thrown
    tested.releaseJobIdLock(configuration);
  }

  @Test
  public void testTaskIdLockAcquire() {
    int tasksCount = 100;
    for (int i = 0; i < tasksCount; i++) {
      TaskID taskID = tested.acquireTaskIdLock(configuration);
      assertTrue(isFileExists(getTaskIdPath(taskID)));
    }

    String jobFolderName = getFileInJobFolder("");
    File jobFolder = new File(jobFolderName);
    assertTrue(jobFolder.isDirectory());
    // we have to multiply by 2 because crc files exists
    assertEquals(tasksCount * 2, jobFolder.list().length);
  }

  @Test
  public void testTaskAttemptIdAcquire() {
    int tasksCount = 100;
    int taskId = 25;

    for (int i = 0; i < tasksCount; i++) {
      TaskAttemptID taskAttemptID = tested.acquireTaskAttemptIdLock(configuration, taskId);
      assertTrue(isFileExists(getTaskAttemptIdPath(taskId, taskAttemptID.getId())));
    }
  }

  @Test
  public void testCatchingRemoteException() throws IOException {
    FileSystem mockedFileSystem = Mockito.mock(FileSystem.class);
    RemoteException thrownException =
        new RemoteException(AlreadyBeingCreatedException.class.getName(), "Failed to CREATE_FILE");
    Mockito.when(mockedFileSystem.createNewFile(Mockito.any())).thenThrow(thrownException);

    HDFSSynchronization synchronization =
        new HDFSSynchronization("someDir", (conf) -> mockedFileSystem);

    assertFalse(synchronization.tryAcquireJobLock(configuration));
  }

  private String getTaskAttemptIdPath(int taskId, int taskAttemptId) {
    return getFileInJobFolder(taskId + "_" + taskAttemptId);
  }

  private String getTaskIdPath(TaskID taskID) {
    return getFileInJobFolder(String.valueOf(taskID.getId()));
  }

  private String getJobLockPath() {
    return getFileInJobFolder("_job");
  }

  private String getFileInJobFolder(String filename) {
    return tmpFolder.getRoot().getAbsolutePath()
        + File.separator
        + DEFAULT_JOB_ID
        + File.separator
        + filename;
  }

  private boolean isFileExists(String path) {
    File file = new File(path);
    return file.exists() && !file.isDirectory();
  }

  private boolean isFolderExists(String path) {
    File file = new File(path);
    return file.exists();
  }
}
