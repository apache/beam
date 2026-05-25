buildscript {
  def getInputParam = { String name ->
      def envVarName = name.toUpperCase().replace('.', '_').replace('-', '_')
      return System.getProperty(name) ?: System.getenv(envVarName)
  }
  def pluginRepositoryUrl = getInputParam('gradle.plugin-repository.url') ?: 'https://plugins.gradle.org/m2'
  def pluginRepositoryUsername = getInputParam('gradle.plugin-repository.username')
  def pluginRepositoryPassword = getInputParam('gradle.plugin-repository.password')
  def dependencyGraphPluginVersion = getInputParam('dependency-graph-plugin.version') ?: '1.4.0'

  logger.lifecycle("Resolving dependency graph plugin ${dependencyGraphPluginVersion} from plugin repository: ${pluginRepositoryUrl}")
  repositories {
    maven { 
      url = pluginRepositoryUrl
      if (pluginRepositoryUsername && pluginRepositoryPassword) {
        logger.lifecycle("Applying credentials for plugin repository: ${pluginRepositoryUrl}")
        credentials {
          username = pluginRepositoryUsername
          password = pluginRepositoryPassword
        }
        authentication {
          basic(BasicAuthentication)
        }
      }
    }
  }
  dependencies {
    classpath "org.gradle:github-dependency-graph-gradle-plugin:${dependencyGraphPluginVersion}"
  }
}

apply plugin: org.gradle.github.GitHubDependencyGraphPlugin
