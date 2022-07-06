plugins {
    base
    // check which dependencies need updating - including Gradle
    // https://github.com/ben-manes/gradle-versions-plugin
    id("com.github.ben-manes.versions") version "0.33.0"
    // top level licence enforcement analysis
    id("org.nosphere.apache.rat") version "0.7.0"
    // release management via gradle
    id("net.researchgate.release") version "2.8.1"
    id("org.apache.beam.module")
    id("org.sonarqube") version "3.0"
}