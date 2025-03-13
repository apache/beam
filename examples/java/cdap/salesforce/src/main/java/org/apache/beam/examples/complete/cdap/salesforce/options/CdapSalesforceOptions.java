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
package org.apache.beam.examples.complete.cdap.salesforce.options;

import io.cdap.plugin.salesforce.SalesforceConstants;
import org.apache.beam.examples.complete.cdap.options.BaseCdapOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link CdapSalesforceOptions} interface provides the custom execution options passed by the
 * executor at the command-line for example with Cdap Salesfroce plugins.
 */
public interface CdapSalesforceOptions extends BaseCdapOptions {

  @Validation.Required
  @Description(SalesforceConstants.PROPERTY_USERNAME)
  String getUsername();

  void setUsername(String username);

  @Validation.Required
  @Description(SalesforceConstants.PROPERTY_PASSWORD)
  String getPassword();

  void setPassword(String password);

  @Validation.Required
  @Description(SalesforceConstants.PROPERTY_SECURITY_TOKEN)
  String getSecurityToken();

  void setSecurityToken(String securityToken);

  @Validation.Required
  @Description(SalesforceConstants.PROPERTY_CONSUMER_KEY)
  String getConsumerKey();

  void setConsumerKey(String consumerKey);

  @Validation.Required
  @Description(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
  String getConsumerSecret();

  void setConsumerSecret(String consumerSecret);

  @Validation.Required
  @Description(SalesforceConstants.PROPERTY_LOGIN_URL)
  String getLoginUrl();

  void setLoginUrl(String loginUrl);
}
