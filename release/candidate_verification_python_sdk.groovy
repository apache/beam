import ReleaseConfiguration
import TestScripts

/*
 * This file will verify Apache/Beam python SDK by following steps:
 *
 * 1. Download files from RC staging location
 * 2. Verify hashes
 * 3. Create a new virtualenv and install the SDK
 * 4. Run Wordcount examples with DirectRunner
 * 5. Run Wordcount examples with DataflowRunner
 * 6. TODO: Run streaming wordcount on DirectRunner and (verify results, how?)
 * 7. TODO: ...
 */

def t = new TestScripts()
t.describe 'Run Apache Beam Python SDK Candidate Verification'
StringBuilder cmd = new StringBuilder()

/*
* 1. Download files from RC staging location, including:
*   apache-beam-2.3.0-python.zip.md5
*   apache-beam-2.3.0-python.zip.sha1
*
* */
t.run("pwd")
t.run("wget ${ReleaseConfiguration.CANDIDATE_URL}${ReleaseConfiguration.SHA1_FILE_NAME}")
t.run("wget ${ReleaseConfiguration.CANDIDATE_URL}${ReleaseConfiguration.MD5_FILE_NAME}")
t.run("wget ${ReleaseConfiguration.CANDIDATE_URL}${ReleaseConfiguration.BEAM_PYTHON_SDK}")
t.run("wget ${ReleaseConfiguration.CANDIDATE_URL}${ReleaseConfiguration.BEAM_PYTHON_RELEASE}")


/*
* 2. Verify hashes
*
* */
t.run("sha1sum -c ${ReleaseConfiguration.SHA1_FILE_NAME}")
t.see("OK")
t.run("md5sum -c ${ReleaseConfiguration.MD5_FILE_NAME}")
t.see("OK")


/*
* 3. create a new virtualenv and install the SDK
*
* */
println("-----------------------------------------------------------")
println("Creating new virtualenv and installing the SDK")
println("-----------------------------------------------------------")
t.run("virtualenv temp_virtualenv")
t.run(". temp_virtualenv/bin/activate")
t.run("unzip ${ReleaseConfiguration.BEAM_PYTHON_RELEASE}")
t.run("cd apache-beam-${ReleaseConfiguration.VERSION}/sdks/python/")
t.run("python setup.py sdist")
t.run("pip install dist/apache-beam-${ReleaseConfiguration.VERSION}.tar.gz[gcp]")
println()


/*
* 4. Run wordcount with DirectRunner
*
* */
println("----------------------------------------------------------------")
println("Running wordcount example with DirectRunner and verify results")
println("----------------------------------------------------------------")
t.run("python -m apache_beam.examples.wordcount --output wordcount_direct.txt")
t.run "ls"
t.see "wordcount_direct.txt-00000-of-00001"
println()


/*
* 5. Run wordcount with DataflowRunner
*
* */
cmd.setLength(0) // clear the cmd buffer
cmd.append("python -m apache_beam.examples.wordcount ")
    .append("--output gs://${ReleaseConfiguration.BUCKET_NAME}/${ReleaseConfiguration.WORDCOUNT_OUTPUT} ")
    .append("--staging_location gs://${ReleaseConfiguration.BUCKET_NAME}${ReleaseConfiguration.TEMP_DIR} ")
    .append("--temp_location gs://${ReleaseConfiguration.BUCKET_NAME}${ReleaseConfiguration.TEMP_DIR} ")
    .append("--runner DataflowRunner ")
    .append("--job_name wordcount ")
    .append("--project ${ReleaseConfiguration.PROJECT_ID} ")
    .append("--num_workers ${ReleaseConfiguration.NUM_WORKERS} ")
    .append("--sdk_location dist/apache-beam-${ReleaseConfiguration.VERSION}.tar.gz ")
println("----------------------------------------------------------------")
println("Running wordcount example with DataflowRunner with command:")
println(cmd.toString())
println("----------------------------------------------------------------")
t.run(cmd.toString())
//TODO: verify results.


/*
* TODO: 6. Run Streaming wordcount with DirectRunner
*
* */











