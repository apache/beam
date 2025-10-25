plugins {
  id("org.apache.beam.module")
  id("java-library")
}

description = "Apache Beam :: SDKs :: Java :: ML :: RemoteInference"

dependencies {
  // Core Beam SDK
  implementation(project(":sdks:java:core"))

  implementation("com.openai:openai-java:4.3.0")
  implementation("com.google.auto.value:auto-value:1.11.0")
  implementation("com.google.auto.value:auto-value-annotations:1.11.0")

  // testing
  testImplementation("junit:junit:4.13.2")
  testImplementation(project(":sdks:java:testing:test-utils"))
}

