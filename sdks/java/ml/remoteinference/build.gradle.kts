plugins {
  id("org.apache.beam.module")
  id("java-library")
}

description = "Apache Beam :: SDKs :: Java :: ML :: RemoteInference"

dependencies {
  // Core Beam SDK
  implementation(project(":sdks:java:core"))


  // testing
  testImplementation("junit:junit:4.13.2")
  testImplementation(project(":sdks:java:testing:test-utils"))
}

