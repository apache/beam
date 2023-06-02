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
import 'package:playground_components/playground_components.dart';

import 'auth/notifier.dart';
import 'cache/content_tree.dart';
import 'cache/sdk.dart';
import 'cache/unit_content.dart';
import 'cache/unit_progress.dart';
import 'pages/welcome/page.dart';
import 'repositories/client/client.dart';
import 'repositories/client/cloud_functions_client.dart';
import 'router/page_factory.dart';
import 'router/route_information_parser.dart';
import 'state.dart';

Future<void> initializeServiceLocator() async {
  await _initializeRepositories();
  _initializeAuth();
  _initializeState();
  _initializeServices();
  _initializeCaches();
}

Future<void> _initializeRepositories() async {
  final routerUrl = await getRouterUrl();

  final codeClient = GrpcCodeClient(
    url: routerUrl,
    // TODO(nausharipov): Remove the hardcoded SDKs when runners are hidden.
    runnerUrlsById: {
      Sdk.java.id: await getRunnerUrl(Sdk.java),
      Sdk.go.id: await getRunnerUrl(Sdk.go),
      Sdk.python.id: await getRunnerUrl(Sdk.python),
    },
  );
  final exampleClient = GrpcExampleClient(url: routerUrl);

  GetIt.instance.registerSingleton<CodeClient>(codeClient);
  GetIt.instance.registerSingleton<ExampleClient>(exampleClient);
  GetIt.instance.registerSingleton(ExampleRepository(client: exampleClient));
}

void _initializeAuth() {
  GetIt.instance.registerSingleton(AuthNotifier());
}

void _initializeCaches() {
  final client = CloudFunctionsTobClient();

  GetIt.instance.registerSingleton<TobClient>(client);
  GetIt.instance.registerSingleton(ContentTreeCache(client: client));
  GetIt.instance.registerSingleton(SdkCache(client: client));
  GetIt.instance.registerSingleton(UnitContentCache(client: client));
  GetIt.instance.registerSingleton(UnitProgressCache());
}

void _initializeState() {
  final pageStack = PageStack(
    bottomPage: WelcomePage(),
    createPage: PageFactory.createPage,
    routeInformationParser: TobRouteInformationParser(),
  );
  GetIt.instance.registerSingleton(AppNotifier());
  GetIt.instance.registerSingleton(pageStack);
  GetIt.instance.registerSingleton(BeamRouterDelegate(pageStack));
}

void _initializeServices() {
  final analyticsService = BeamGoogleAnalytics4Service(
    measurementId: getGoogleAnalyticsMeasurementId(),
  );
  GetIt.instance.registerSingleton<BeamAnalyticsService>(analyticsService);
}
