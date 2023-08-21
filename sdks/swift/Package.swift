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

import PackageDescription

let dependencies: [Package.Dependency] = [
    // Core Dependencies
    .package(url: "https://github.com/grpc/grpc-swift.git", from: "1.19.0"),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    
    // Additional Transform Dependencies
    .package(url: "https://github.com/awslabs/aws-sdk-swift.git", from: "0.23.0"),
    .package(url: "https://github.com/googleapis/google-auth-library-swift",from:"0.0.0"),
    
    
    // Swift Package Manager Plugins
    .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0")
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
    ],
    dependencies: dependencies,
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "ApacheBeam",
            dependencies: [
                .product(name:"GRPC",package:"grpc-swift"),
                .product(name: "Logging",package:"swift-log"),
                .product(name: "AWSS3",package:"aws-sdk-swift")
            ]
        ),
        .testTarget(
            name: "ApacheBeamTests",
            dependencies: ["ApacheBeam"]),
    ]
)
