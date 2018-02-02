
class ReleaseConfiguration {

    // Python Release Candidate
    static final String VERSION             = '2.3.0'
    static final String CANDIDATE_URL       = "https://dist.apache.org/repos/dist/dev/beam/${this.VERSION}/"
    static final String SHA1_FILE_NAME      = "apache-beam-${this.VERSION}-python.zip.sha1"
    static final String MD5_FILE_NAME       = "apache-beam-${this.VERSION}-python.zip.md5"
    static final String BEAM_PYTHON_SDK     = "apache-beam-${this.VERSION}-python.zip"
    static final String BEAM_PYTHON_RELEASE = "apache-beam-${this.VERSION}-source-release.zip"


    // Cloud Configurations
    static final String PROJECT_ID            = 'my-first-project-190318'
    static final String BUCKET_NAME           = 'yifan_auto_verification_test_bucket'
    static final String TEMP_DIR              = '/temp'
    static final int NUM_WORKERS              = 1
    static final String GAME_DATA             = '5000_gaming_data.csv'
    static final String WORDCOUNT_OUTPUT      = 'wordcount_direct.txt'

}