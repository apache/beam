locals {
  looker_jar_metadata_keys = {
    url            = "url"
    sha256         = "sha256"
    version_text   = "version_text"
    depSha256      = "depSha256"
    depUrl         = "depUrl"
    depDisplayFile = "depDisplayFile"
  }
  temporary = {
    looker = "/tmp/looker"
  }
  prometheus = {
    url         = "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${var.prometheus_version}/jmx_prometheus_javaagent-${var.prometheus_version}.jar"
    target_path = "${local.temporary.looker}/jmx_prometheus_javaagent.jar"
  }
}
