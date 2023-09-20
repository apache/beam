// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

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
import CompilerPluginSupport
import PackageDescription

let dependencies: [Package.Dependency] = [
    // Core Dependencies
    .package(url: "https://github.com/grpc/grpc-swift.git", from: "1.19.0"),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    .package(url: "https://github.com/apple/swift-argument-parser", from: "1.2.0"),
    .package(url: "https://github.com/apple/swift-metrics.git", from: "2.0.0"),
    
    // Additional Transform Dependencies
    .package(url: "https://github.com/awslabs/aws-sdk-swift.git", from: "0.23.0"),
    .package(url: "https://github.com/googleapis/google-auth-library-swift",from:"0.0.0"),
    .package(url: "https://github.com/duckdb/duckdb-swift", .upToNextMinor(from: .init(0, 8, 0))),
    .package(url: "https://github.com/pvieito/PythonKit.git", branch: "master"),
   
    // Swift Macro Support
    .package(url: "https://github.com/apple/swift-syntax.git", branch: "main"), 

    // Swift Package Manager Plugins
    .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0"),
    .package(url: "https://github.com/nicklockwood/SwiftFormat", from: "0.52.3"),
]

let package = Package(
    name: "ApacheBeam",
    platforms: [
        .macOS("13.0")
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "ApacheBeam",
            targets: ["ApacheBeam"]),
        .library(
            name: "DuckDBIO",
            targets: ["DuckDBIO"]
        ),
        .executable(
            name: "Wordcount",
            targets: ["Wordcount"])
    ],
    dependencies: dependencies,
    targets: [
        .macro(
            name: "ApacheBeamPlugin",
            dependencies: [
              .product(name: "SwiftSyntax", package: "swift-syntax"),
              .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
              .product(name: "SwiftOperators", package: "swift-syntax"),
              .product(name: "SwiftParser", package: "swift-syntax"),
              .product(name: "SwiftParserDiagnostics", package: "swift-syntax"),
              .product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
              ]
        ),
        .target(
            name: "ApacheBeam",
            dependencies: [
//                "ApacheBeamPlugin", // This is disabled until it is supported on Linux
                .product(name: "GRPC",package:"grpc-swift"),
                .product(name: "Logging",package:"swift-log"),
                .product(name: "AWSS3",package:"aws-sdk-swift"),
                .product(name: "OAuth2", package:"google-auth-library-swift"),
                .product(name: "ArgumentParser", package:"swift-argument-parser")
            ]
        ),
        .target(
            name:"DuckDBIO",
            dependencies: [
                "ApacheBeam",
                .product(name: "DuckDB",package:"duckdb-swift")
            ]
        ),
        .executableTarget(
            name:"Wordcount",
            dependencies: ["ApacheBeam",
                           .product(name: "ArgumentParser", package:"swift-argument-parser")],
            path:"Sources/Examples/Wordcount"), 
        .testTarget(
            name: "ApacheBeamTests",
            dependencies: ["ApacheBeam"],
            exclude:["Pipeline/Fixtures/file1.txt","Pipeline/Fixtures/file2.txt"]
        ),
    ]
)
