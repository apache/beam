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
package org.apache.beam.sdk.io.aws.sns;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.AddPermissionRequest;
import com.amazonaws.services.sns.model.AddPermissionResult;
import com.amazonaws.services.sns.model.CheckIfPhoneNumberIsOptedOutRequest;
import com.amazonaws.services.sns.model.CheckIfPhoneNumberIsOptedOutResult;
import com.amazonaws.services.sns.model.ConfirmSubscriptionRequest;
import com.amazonaws.services.sns.model.ConfirmSubscriptionResult;
import com.amazonaws.services.sns.model.CreatePlatformApplicationRequest;
import com.amazonaws.services.sns.model.CreatePlatformApplicationResult;
import com.amazonaws.services.sns.model.CreatePlatformEndpointRequest;
import com.amazonaws.services.sns.model.CreatePlatformEndpointResult;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteEndpointRequest;
import com.amazonaws.services.sns.model.DeleteEndpointResult;
import com.amazonaws.services.sns.model.DeletePlatformApplicationRequest;
import com.amazonaws.services.sns.model.DeletePlatformApplicationResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.DeleteTopicResult;
import com.amazonaws.services.sns.model.GetEndpointAttributesRequest;
import com.amazonaws.services.sns.model.GetEndpointAttributesResult;
import com.amazonaws.services.sns.model.GetPlatformApplicationAttributesRequest;
import com.amazonaws.services.sns.model.GetPlatformApplicationAttributesResult;
import com.amazonaws.services.sns.model.GetSMSAttributesRequest;
import com.amazonaws.services.sns.model.GetSMSAttributesResult;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.ListEndpointsByPlatformApplicationRequest;
import com.amazonaws.services.sns.model.ListEndpointsByPlatformApplicationResult;
import com.amazonaws.services.sns.model.ListPhoneNumbersOptedOutRequest;
import com.amazonaws.services.sns.model.ListPhoneNumbersOptedOutResult;
import com.amazonaws.services.sns.model.ListPlatformApplicationsRequest;
import com.amazonaws.services.sns.model.ListPlatformApplicationsResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.ListSubscriptionsRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsResult;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.OptInPhoneNumberRequest;
import com.amazonaws.services.sns.model.OptInPhoneNumberResult;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.RemovePermissionRequest;
import com.amazonaws.services.sns.model.RemovePermissionResult;
import com.amazonaws.services.sns.model.SetEndpointAttributesRequest;
import com.amazonaws.services.sns.model.SetEndpointAttributesResult;
import com.amazonaws.services.sns.model.SetPlatformApplicationAttributesRequest;
import com.amazonaws.services.sns.model.SetPlatformApplicationAttributesResult;
import com.amazonaws.services.sns.model.SetSMSAttributesRequest;
import com.amazonaws.services.sns.model.SetSMSAttributesResult;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SetTopicAttributesResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sns.model.UnsubscribeResult;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import org.mockito.Mockito;

/** Mock class to test amazon sns service. */
public abstract class AmazonSNSMock implements AmazonSNS, Serializable {

  public AmazonSNSMock() {}

  @Override
  public void setEndpoint(String endpoint) {}

  @Override
  public void setRegion(Region region) {}

  @Override
  public AddPermissionResult addPermission(AddPermissionRequest addPermissionRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public AddPermissionResult addPermission(
      String topicArn, String label, List<String> aWSAccountIds, List<String> actionNames) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public CheckIfPhoneNumberIsOptedOutResult checkIfPhoneNumberIsOptedOut(
      CheckIfPhoneNumberIsOptedOutRequest checkIfPhoneNumberIsOptedOutRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ConfirmSubscriptionResult confirmSubscription(
      ConfirmSubscriptionRequest confirmSubscriptionRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ConfirmSubscriptionResult confirmSubscription(
      String topicArn, String token, String authenticateOnUnsubscribe) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ConfirmSubscriptionResult confirmSubscription(String topicArn, String token) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public CreatePlatformApplicationResult createPlatformApplication(
      CreatePlatformApplicationRequest createPlatformApplicationRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public CreatePlatformEndpointResult createPlatformEndpoint(
      CreatePlatformEndpointRequest createPlatformEndpointRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public CreateTopicResult createTopic(CreateTopicRequest createTopicRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public CreateTopicResult createTopic(String name) {
    return new CreateTopicResult().withTopicArn(name);
  }

  @Override
  public DeleteEndpointResult deleteEndpoint(DeleteEndpointRequest deleteEndpointRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DeletePlatformApplicationResult deletePlatformApplication(
      DeletePlatformApplicationRequest deletePlatformApplicationRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DeleteTopicResult deleteTopic(DeleteTopicRequest deleteTopicRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DeleteTopicResult deleteTopic(String topicArn) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetEndpointAttributesResult getEndpointAttributes(
      GetEndpointAttributesRequest getEndpointAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetPlatformApplicationAttributesResult getPlatformApplicationAttributes(
      GetPlatformApplicationAttributesRequest getPlatformApplicationAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetSMSAttributesResult getSMSAttributes(GetSMSAttributesRequest getSMSAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetSubscriptionAttributesResult getSubscriptionAttributes(
      GetSubscriptionAttributesRequest getSubscriptionAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetSubscriptionAttributesResult getSubscriptionAttributes(String subscriptionArn) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetTopicAttributesResult getTopicAttributes(
      GetTopicAttributesRequest getTopicAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetTopicAttributesResult getTopicAttributes(String topicArn) {
    GetTopicAttributesResult result = Mockito.mock(GetTopicAttributesResult.class);
    SdkHttpMetadata metadata = Mockito.mock(SdkHttpMetadata.class);
    Mockito.when(metadata.getHttpHeaders()).thenReturn(new HashMap<>());
    Mockito.when(metadata.getHttpStatusCode()).thenReturn(200);
    Mockito.when(result.getSdkHttpMetadata()).thenReturn(metadata);
    return result;
  }

  @Override
  public ListEndpointsByPlatformApplicationResult listEndpointsByPlatformApplication(
      ListEndpointsByPlatformApplicationRequest listEndpointsByPlatformApplicationRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListPhoneNumbersOptedOutResult listPhoneNumbersOptedOut(
      ListPhoneNumbersOptedOutRequest listPhoneNumbersOptedOutRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListPlatformApplicationsResult listPlatformApplications(
      ListPlatformApplicationsRequest listPlatformApplicationsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListPlatformApplicationsResult listPlatformApplications() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListSubscriptionsResult listSubscriptions(
      ListSubscriptionsRequest listSubscriptionsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListSubscriptionsResult listSubscriptions() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListSubscriptionsResult listSubscriptions(String nextToken) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListSubscriptionsByTopicResult listSubscriptionsByTopic(
      ListSubscriptionsByTopicRequest listSubscriptionsByTopicRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListSubscriptionsByTopicResult listSubscriptionsByTopic(String topicArn) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListSubscriptionsByTopicResult listSubscriptionsByTopic(
      String topicArn, String nextToken) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListTopicsResult listTopics(ListTopicsRequest listTopicsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListTopicsResult listTopics() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListTopicsResult listTopics(String nextToken) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public OptInPhoneNumberResult optInPhoneNumber(OptInPhoneNumberRequest optInPhoneNumberRequest) {
    return null;
  }

  @Override
  public PublishResult publish(String topicArn, String message) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public PublishResult publish(String topicArn, String message, String subject) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public RemovePermissionResult removePermission(RemovePermissionRequest removePermissionRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public RemovePermissionResult removePermission(String topicArn, String label) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SetEndpointAttributesResult setEndpointAttributes(
      SetEndpointAttributesRequest setEndpointAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SetPlatformApplicationAttributesResult setPlatformApplicationAttributes(
      SetPlatformApplicationAttributesRequest setPlatformApplicationAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SetSMSAttributesResult setSMSAttributes(SetSMSAttributesRequest setSMSAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SetSubscriptionAttributesResult setSubscriptionAttributes(
      SetSubscriptionAttributesRequest setSubscriptionAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SetSubscriptionAttributesResult setSubscriptionAttributes(
      String subscriptionArn, String attributeName, String attributeValue) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SetTopicAttributesResult setTopicAttributes(
      SetTopicAttributesRequest setTopicAttributesRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SetTopicAttributesResult setTopicAttributes(
      String topicArn, String attributeName, String attributeValue) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SubscribeResult subscribe(SubscribeRequest subscribeRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SubscribeResult subscribe(String topicArn, String protocol, String endpoint) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public UnsubscribeResult unsubscribe(UnsubscribeRequest unsubscribeRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public UnsubscribeResult unsubscribe(String subscriptionArn) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void shutdown() {}

  @Override
  public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    throw new RuntimeException("Not implemented");
  }
}
