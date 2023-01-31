/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.spanner;

import static com.google.cloud.spanner.Value.int64;
import static com.google.cloud.spanner.Value.string;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link DefaultSpannerResourceManager}. */
@RunWith(JUnit4.class)
public final class DefaultSpannerResourceManagerTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Spanner spanner;

  @Mock private Database database;
  @Mock private Instance instance;
  @Mock private InstanceAdminClient instanceAdminClient;
  @Mock private ResultSet resultSet;

  private static final String TEST_ID = "test";
  private static final String PROJECT_ID = "test-project";
  private static final String REGION = "us-east1";
  private DefaultSpannerResourceManager testManager;

  @Captor private ArgumentCaptor<Iterable<Mutation>> writeMutationCaptor;
  @Captor private ArgumentCaptor<Iterable<String>> statementCaptor;
  @Captor private ArgumentCaptor<String> instanceIdCaptor;
  @Captor private ArgumentCaptor<String> databaseIdCaptor;

  @Before
  public void setUp() {
    testManager = new DefaultSpannerResourceManager(spanner, TEST_ID, PROJECT_ID, REGION);
  }

  private void prepareCreateInstanceMock() throws ExecutionException, InterruptedException {
    when(spanner.getInstanceAdminClient().createInstance(any()).get()).thenReturn(instance);
  }

  @Test
  public void testCreateTableShouldThrowExceptionWhenSpannerCreateInstanceFails()
      throws ExecutionException, InterruptedException {
    // arrange
    when(spanner.getInstanceAdminClient().createInstance(any()).get())
        .thenThrow(InterruptedException.class);
    prepareCreateDatabaseMock();
    prepareUpdateDatabaseMock();
    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId   INT64 NOT NULL,\n"
            + "  FirstName  STRING(1024),\n"
            + "  LastName   STRING(1024),\n"
            + ") PRIMARY KEY (SingerId)";

    // act & assert
    assertThrows(SpannerResourceManagerException.class, () -> testManager.createTable(statement));
  }

  @Test
  public void testCreateTableShouldThrowExceptionWhenSpannerCreateDatabaseFails()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareCreateInstanceMock();
    when(spanner.getDatabaseAdminClient().createDatabase(any(), any(), any()).get())
        .thenThrow(InterruptedException.class);
    prepareUpdateDatabaseMock();
    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId   INT64 NOT NULL,\n"
            + "  FirstName  STRING(1024),\n"
            + "  LastName   STRING(1024),\n"
            + ") PRIMARY KEY (SingerId)";

    // act & assert
    assertThrows(SpannerResourceManagerException.class, () -> testManager.createTable(statement));
  }

  @Test
  public void testCreateTableShouldThrowExceptionWhenSpannerUpdateDatabaseFails()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareCreateInstanceMock();
    prepareCreateDatabaseMock();
    when(spanner.getDatabaseAdminClient().updateDatabaseDdl(any(), any(), any(), any()).get())
        .thenThrow(InterruptedException.class);
    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId   INT64 NOT NULL,\n"
            + "  FirstName  STRING(1024),\n"
            + "  LastName   STRING(1024),\n"
            + ") PRIMARY KEY (SingerId)";

    // act & assert
    assertThrows(SpannerResourceManagerException.class, () -> testManager.createTable(statement));
  }

  @Test
  public void testCreateTableShouldWorkWhenSpannerDoesntThrowAnyError()
      throws ExecutionException, InterruptedException {
    //   arrange
    prepareCreateInstanceMock();
    prepareCreateDatabaseMock();
    prepareUpdateDatabaseMock();
    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId   INT64 NOT NULL,\n"
            + "  FirstName  STRING(1024),\n"
            + "  LastName   STRING(1024),\n"
            + ") PRIMARY KEY (SingerId)";

    // act
    testManager.createTable(statement);

    // assert
    // verify createInstance, createDatabase, and updateDatabaseDdl were called twice - once in
    // create table, once in their respective prepareMock helper methods.
    verify(spanner.getInstanceAdminClient(), times(2)).createInstance(any());
    verify(spanner.getDatabaseAdminClient(), times(2)).createDatabase(any(), any(), any());
    verify(spanner.getDatabaseAdminClient(), times(2))
        .updateDatabaseDdl(
            instanceIdCaptor.capture(),
            databaseIdCaptor.capture(),
            statementCaptor.capture(),
            any());

    String actualInstanceId = instanceIdCaptor.getValue();
    String actualDatabaseId = databaseIdCaptor.getValue();
    Iterable<String> actualStatement = statementCaptor.getValue();

    assertThat(actualInstanceId).matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");

    assertThat(actualDatabaseId).isEqualTo(TEST_ID);
    assertThat(actualStatement).containsExactlyElementsIn(ImmutableList.of(statement));
  }

  @Test
  public void testWriteSingleRecordShouldWorkWhenSpannerWriteSucceeds()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareTable();
    when(spanner.getDatabaseClient(any()).write(any())).thenReturn(Timestamp.now());
    // spotless:off
    Mutation testMutation =
        Mutation.newInsertOrUpdateBuilder("SingerId")
            .set("SingerId").to(1)
            .set("FirstName").to("Marc")
            .set("LastName").to("Richards")
            .build();
    // spotless:on

    // act
    testManager.write(testMutation);

    // assert
    verify(spanner.getDatabaseClient(any())).write(writeMutationCaptor.capture());
    Iterable<Mutation> actualWriteMutation = writeMutationCaptor.getValue();
    assertThat(actualWriteMutation).containsExactlyElementsIn(ImmutableList.of(testMutation));
  }

  @Test
  public void testWriteSingleRecordShouldThrowExceptionWhenCalledBeforeCreateTable() {
    // arrange
    // spotless:off
    Mutation testMutation =
        Mutation.newInsertOrUpdateBuilder("SingerId")
            .set("SingerId").to(1)
            .set("FirstName").to("Marc")
            .set("LastName").to("Richards")
            .build();
    // spotless:on

    // act & assert
    assertThrows(IllegalStateException.class, () -> testManager.write(testMutation));
  }

  @Test
  public void testWriteSingleRecordShouldThrowExceptionWhenSpannerWriteFails()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareTable();
    when(spanner.getDatabaseClient(any()).write(any())).thenThrow(SpannerException.class);
    // spotless:off
    Mutation testMutation =
        Mutation.newInsertOrUpdateBuilder("SingerId")
            .set("SingerId").to(1)
            .set("FirstName").to("Marc")
            .set("LastName").to("Richards")
            .build();
    // spotless:on

    // act & assert
    assertThrows(SpannerResourceManagerException.class, () -> testManager.write(testMutation));
  }

  @Test
  public void testWriteMultipleRecordsShouldWorkWhenSpannerWriteSucceeds()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareTable();
    when(spanner.getDatabaseClient(any()).write(any())).thenReturn(Timestamp.now());
    // spotless:off
    ImmutableList<Mutation> testMutations =
        ImmutableList.of(
            Mutation.newInsertOrUpdateBuilder("SingerId")
                .set("SingerId").to(1)
                .set("FirstName").to("Marc")
                .set("LastName").to("Richards")
                .build(),
            Mutation.newInsertOrUpdateBuilder("SingerId")
                .set("SingerId").to(2)
                .set("FirstName").to("Catalina")
                .set("LastName").to("Smith")
                .build());
    // spotless:on

    // act
    testManager.write(testMutations);

    // assert
    verify(spanner.getDatabaseClient(any())).write(writeMutationCaptor.capture());
    Iterable<Mutation> actualWriteMutation = writeMutationCaptor.getValue();

    assertThat(actualWriteMutation).containsExactlyElementsIn(testMutations);
  }

  @Test
  public void testWriteMultipleRecordsShouldThrowExceptionWhenCalledBeforeCreateTable() {
    // arrange
    // spotless:off
    ImmutableList<Mutation> testMutations =
        ImmutableList.of(
            Mutation.newInsertOrUpdateBuilder("SingerId")
                .set("SingerId").to(1)
                .set("FirstName").to("Marc")
                .set("LastName").to("Richards")
                .build(),
            Mutation.newInsertOrUpdateBuilder("SingerId")
                .set("SingerId").to(2)
                .set("FirstName").to("Catalina")
                .set("LastName").to("Smith")
                .build());
    // spotless:on

    // act & assert
    assertThrows(IllegalStateException.class, () -> testManager.write(testMutations));
  }

  @Test
  public void testWriteMultipleRecordsShouldThrowExceptionWhenSpannerWriteFails()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareTable();
    when(spanner.getDatabaseClient(any()).write(any())).thenThrow(SpannerException.class);
    // spotless:off
    ImmutableList<Mutation> testMutations =
        ImmutableList.of(
            Mutation.newInsertOrUpdateBuilder("SingerId")
                .set("SingerId").to(1)
                .set("FirstName").to("Marc")
                .set("LastName").to("Richards")
                .build(),
            Mutation.newInsertOrUpdateBuilder("SingerId")
                .set("SingerId").to(2)
                .set("FirstName").to("Catalina")
                .set("LastName").to("Smith")
                .build());
    // spotless:on

    // act & assert
    assertThrows(SpannerResourceManagerException.class, () -> testManager.write(testMutations));
  }

  @Test
  public void testReadRecordsShouldWorkWhenSpannerReadSucceeds()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareTable();
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    // spotless:off
    Struct struct1 =
        Struct.newBuilder()
            .set("SingerId").to(int64(1))
            .set("FirstName").to(string("Marc"))
            .set("LastName").to(string("Richards"))
            .build();
    Struct struct2 =
        Struct.newBuilder()
            .set("SingerId").to(int64(2))
            .set("FirstName").to(string("Catalina"))
            .set("LastName").to(string("Smith"))
            .build();
    // spotless:on
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct1).thenReturn(struct2);
    when(spanner.getDatabaseClient(any()).singleUse().read(any(), any(), any()))
        .thenReturn(resultSet);

    // act
    ImmutableList<Struct> actual =
        testManager.readTableRecords("Singers", "SingerId", "FirstName", "LastName");

    // assert
    ImmutableList<Struct> expected = ImmutableList.of(struct1, struct2);
    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @Test
  public void testReadRecordsWithListOfColumnNamesShouldWorkWhenSpannerReadSucceeds()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareTable();
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    // spotless:off
    Struct struct =
        Struct.newBuilder()
            .set("SingerId").to(int64(1))
            .set("FirstName").to(string("Marc"))
            .set("LastName").to(string("Richards"))
            .build();
    // spotless:on
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);
    when(spanner.getDatabaseClient(any()).singleUse().read(any(), any(), any()))
        .thenReturn(resultSet);
    ImmutableList<String> columnNames = ImmutableList.of("SingerId", "FirstName", "LastName");

    // act
    ImmutableList<Struct> actual = testManager.readTableRecords("Singers", columnNames);

    // assert
    ImmutableList<Struct> expected = ImmutableList.of(struct);
    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @Test
  public void testReadRecordsShouldThrowExceptionWhenCalledBeforeCreateTable() {
    ImmutableList<String> columnNames = ImmutableList.of("SingerId");

    assertThrows(
        IllegalStateException.class, () -> testManager.readTableRecords("Singers", columnNames));
    assertThrows(
        IllegalStateException.class, () -> testManager.readTableRecords("Singers", "SingerId"));
  }

  @Test
  public void testReadRecordsShouldThrowExceptionWhenSpannerReadFails()
      throws ExecutionException, InterruptedException {
    // arrange
    prepareTable();
    when(spanner.getDatabaseClient(any()).singleUse().read(any(), any(), any()))
        .thenThrow(SpannerException.class);
    ImmutableList<String> columnNames = ImmutableList.of("SingerId");

    // act & assert
    assertThrows(
        SpannerResourceManagerException.class,
        () -> testManager.readTableRecords("Singers", "SingerId"));
    assertThrows(
        SpannerResourceManagerException.class,
        () -> testManager.readTableRecords("Singers", columnNames));
  }

  @Test
  public void testCleanupAllShouldThrowExceptionWhenSpannerDeleteInstanceFails() {
    // arrange
    doThrow(SpannerException.class).when(instanceAdminClient).deleteInstance(any());
    when(spanner.getInstanceAdminClient()).thenReturn(instanceAdminClient);
    testManager = new DefaultSpannerResourceManager(spanner, TEST_ID, PROJECT_ID, REGION);

    // act & assert
    assertThrows(SpannerResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldWorkWhenSpannerDeleteInstanceSucceeds() {
    // arrange
    doNothing().when(instanceAdminClient).deleteInstance(any());
    when(spanner.getInstanceAdminClient()).thenReturn(instanceAdminClient);
    testManager = new DefaultSpannerResourceManager(spanner, TEST_ID, PROJECT_ID, REGION);

    // act
    testManager.cleanupAll();

    // assert
    verify(spanner.getInstanceAdminClient()).deleteInstance(any());
    verify(spanner).close();
  }

  @Test
  public void testManagerShouldBeUnusableAfterCleanup() {
    // arrange
    doNothing().when(instanceAdminClient).deleteInstance(any());
    when(spanner.getInstanceAdminClient()).thenReturn(instanceAdminClient);
    when(spanner.isClosed()).thenReturn(true);
    testManager = new DefaultSpannerResourceManager(spanner, TEST_ID, PROJECT_ID, REGION);
    testManager.cleanupAll();
    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId   INT64 NOT NULL,\n"
            + "  FirstName  STRING(1024),\n"
            + "  LastName   STRING(1024),\n"
            + ") PRIMARY KEY (SingerId)";
    // spotless:off
    Mutation testMutation =
        Mutation.newInsertOrUpdateBuilder("SingerId")
            .set("SingerId").to(1)
            .set("FirstName").to("Marc")
            .set("LastName").to("Richards")
            .build();
    // spotless:on
    ImmutableList<String> columnNames = ImmutableList.of("SingerId");

    // act & assert
    assertThrows(IllegalStateException.class, () -> testManager.createTable(statement));
    assertThrows(
        IllegalStateException.class, () -> testManager.readTableRecords("Singers", "SingerId"));
    assertThrows(
        IllegalStateException.class, () -> testManager.readTableRecords("Singers", columnNames));
    assertThrows(IllegalStateException.class, () -> testManager.write(testMutation));
    assertThrows(
        IllegalStateException.class, () -> testManager.write(ImmutableList.of(testMutation)));
  }

  private void prepareCreateDatabaseMock() throws ExecutionException, InterruptedException {
    Mockito.lenient()
        .when(spanner.getDatabaseAdminClient().createDatabase(any(), any(), any()).get())
        .thenReturn(database);
  }

  private void prepareUpdateDatabaseMock() throws ExecutionException, InterruptedException {
    Mockito.lenient()
        .when(spanner.getDatabaseAdminClient().updateDatabaseDdl(any(), any(), any(), any()).get())
        .thenReturn(null);
  }

  private void prepareTable() throws ExecutionException, InterruptedException {
    prepareCreateInstanceMock();
    prepareCreateDatabaseMock();
    prepareUpdateDatabaseMock();
    testManager.createTable("");
  }
}
