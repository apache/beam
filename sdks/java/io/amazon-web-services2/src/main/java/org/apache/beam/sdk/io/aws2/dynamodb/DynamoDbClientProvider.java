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
package org.apache.beam.sdk.io.aws2.dynamodb;

import java.io.Serializable;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Provides instances of DynamoDB clients.
 *
 * <p>Please note, that any instance of {@link DynamoDbClientProvider} must be {@link Serializable}
 * to ensure it can be sent to worker machines.
 *
 * @deprecated Configure a custom {@link org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory}
 *     using {@link AwsOptions#getClientBuilderFactory()} instead.
 */
@Deprecated
public interface DynamoDbClientProvider extends Serializable {
  DynamoDbClient getDynamoDbClient();
}
