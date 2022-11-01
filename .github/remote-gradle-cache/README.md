# Remote Gradle Cache 

Remote Gradle Cache is supported by GCP Buckets and uses GCP Credentials allocated in the system for authentication. The plugin used is a custom version of https://github.com/androidx/gcp-gradle-build-cache. 

 Additionally , JSON service account keys can be used as follows.

    remote(GcpBuildCache::class)  {
    
	    projectId =  "apache-beam-testing"
    
	    bucketName =  "gradle-cache-storage"
    
        credentials = ExportedKeyGcpCredentials(File("path/to/credentials.json"))
    
	    isPush = isMasterGHBuild
    
	    isEnabled =  true
    
    }


Remote Cache is enabled for GitHub Workflows based on the existent CI environment variables, external contributors can enabled it by setting the `BEAM_REMOTE_CACHE_ENABLED` environment variable.

In other cases, the settings will fallback to local cache directory as default.

The terraform file included serves as a reference for the bucket configuration, public access is disabled and only authenticated connections are allowed for external contributors.

