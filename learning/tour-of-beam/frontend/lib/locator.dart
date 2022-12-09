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

import 'package:app_state/app_state.dart';
import 'package:get_it/get_it.dart';

import 'cache/content_tree.dart';
import 'cache/sdk.dart';
import 'cache/unit_content.dart';
import 'pages/welcome/page.dart';
import 'repositories/client/cloud_functions_client.dart';
import 'router/page_factory.dart';
import 'router/route_information_parser.dart';

Future<void> initializeServiceLocator() async {
  _initializeCaches();
  _initializeState();
}

void _initializeCaches() {
  final client = CloudFunctionsTobClient();

  GetIt.instance.registerSingleton(ContentTreeCache(client: client));
  GetIt.instance.registerSingleton(SdkCache(client: client));
  GetIt.instance.registerSingleton(UnitContentCache(client: client));
}

void _initializeState() {
  GetIt.instance.registerSingleton(
    PageStack(
      bottomPage: WelcomePage(),
      createPage: PageFactory.createPage,
      routeInformationParser: TobRouteInformationParser(),
    ),
  );
}
