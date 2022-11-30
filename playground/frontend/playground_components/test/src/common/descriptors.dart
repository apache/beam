import 'package:playground_components/playground_components.dart';

import 'examples.dart';

const emptyDescriptor = EmptyExampleLoadingDescriptor(sdk: Sdk.java);

final standardDescriptor1 = StandardExampleLoadingDescriptor(
  path: exampleMock1.path,
);
final contentDescriptor1 = ContentExampleLoadingDescriptor(
  content: exampleMock1.source,
  sdk: exampleMock1.sdk,
  name: exampleMock1.name,
  complexity: exampleMock1.complexity,
);

final standardDescriptor2 = StandardExampleLoadingDescriptor(
  path: exampleMock2.path,
);
final contentDescriptor2 = ContentExampleLoadingDescriptor(
  content: exampleMock2.source,
  sdk: exampleMock2.sdk,
  name: exampleMock2.name,
  complexity: exampleMock2.complexity,
);

final standardGoDescriptor = StandardExampleLoadingDescriptor(
  path: exampleMockGo.path,
);
