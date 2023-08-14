# ``ApacheBeam``

A Swift SDK implementation for Beam

@Metadata {
    @DisplayName("Apache Beam SDK for Swift")
}

## Overview

The Apache Beam SDK for Swift allows Swift developers to create executables that can be submitted to all Beam Portable Runners including Flink and Dataflow. 

To use the Apache Beam SDK for Swift, first add it as a dependency to an executable project:

```swift
let package = Package(
    // name, platforms, products, etc.
    dependencies: [
        // other dependencies
        .package(url: "https://github.com/apache/beam/sdks/swift", from: "2.51.0"),
    ],
    targets: [
        // targets
    ]
)
```

> Note: Swift 5.9 or higher is required in order to use the Swift SDK

## Topics

### Getting Started





