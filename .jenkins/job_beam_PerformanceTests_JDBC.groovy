// This job runs the Beam performance tests on PerfKit Benchmarker.

job('beam_PerformanceTests_JDBC'){
    // Run every 6 hours.
    triggers {
        cron('0 */6 * * *')
    }
    steps {
        // Clones appropriate perfkit branch
        shell('git clone -b apache --single-branch https://github.com/jasonkuster/PerfKitBenchmarker.git')
        python{
            // Runs PerfKit script with appropriate parameters.
            command('PerfKitBenchmarker/pkb.py --project=google.com:clouddfe --benchmarks=beam_integration_benchmark --dpb_it_class=org.apache.beam.sdk.io.Jdbc.JdbcIOIT --dpb_it_args=--tempRoot=gs://temp-storage-for-end-to-end-tests,--project=apache-beam-testing,--postgresServerName=104.154.153.53,--postgresUsername=postgres,--postgresDatabaseName=postgres,--postgresPassword=uuinkks,--postgresSsl=false --dpb_log_level=INFO')
        }
    }
}
