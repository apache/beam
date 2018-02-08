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
 * 6. Run streaming wordcount on DirectRunner
 * 7. Run streaming wordcount on DataflowRunner
 */

def t = new TestScripts()
t.describe 'Run Apache Beam Python SDK Candidate Verification'
StringBuilder cmd = new StringBuilder()

/*
* 1. Download files from RC staging location, including:
*   apache-beam-{version}-python.zip.md5
*   apache-beam-{version}-python.zip.sha1
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
print_separator("Checking sha1 and md5 hashes ")
t.run("sha1sum -c ${ReleaseConfiguration.SHA1_FILE_NAME}")
t.see("OK")
t.run("md5sum -c ${ReleaseConfiguration.MD5_FILE_NAME}")
t.see("OK")


/*
* 3. create a new virtualenv and install the SDK
*
* */
print_separator("Creating new virtualenv and installing the SDK")
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
print_separator("Running wordcount example with DirectRunner and verify results")
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

print_separator("Running wordcount example with DataflowRunner with command: ", cmd.toString())
t.run(cmd.toString())
// verify results.
t.run("gsutil ls gs://${ReleaseConfiguration.BUCKET_NAME}")
4.times {
  t.see("gs://${ReleaseConfiguration.BUCKET_NAME}/${ReleaseConfiguration.WORDCOUNT_OUTPUT}-0000${it}-of-00004")
}

/*
* 6. Run Streaming wordcount with DirectRunner
*
* */
// create pubsub topics (Note that if toipics already exist, there will be errors when running these commands, TODO: error catch)
create_pubsub(t)

cmd.setLength(0) // clear the cmd buffer
cmd.append("python -m apache_beam.examples.streaming_wordcount ")
.append("--input_topic projects/${ReleaseConfiguration.PROJECT_ID}/topics/${ReleaseConfiguration.PUBSUB_TOPIC1} ")
.append("--output_topic projects/${ReleaseConfiguration.PROJECT_ID}/topics/${ReleaseConfiguration.PUBSUB_TOPIC2} ")
.append("--streaming")

print_separator("Running Streaming wordcount example with DirectRunner with command: ", cmd.toString())
def streaming_wordcount_thread = Thread.start(){
  t.run(cmd.toString())
}

t.run("sleep 15")
// verify result
run_pubsub_publish(t)
run_pubsub_pull(t)
t.see("like: 1")
streaming_wordcount_thread.stop()


/*
 * 7. Run Streaming Wordcount with DataflowRunner

* */
cmd.setLength(0) //clear the cmd buffer
cmd.append("python -m apache_beam.examples.streaming_wordcount ")
    .append("--streaming ")
    .append("--job_name pyflow-wordstream-candidate ")
    .append("runner DataflowRunner ")
    .append("--input_topic projects/${ReleaseConfiguration.PROJECT_ID}/topics/${ReleaseConfiguration.PUBSUB_TOPIC1} ")
    .append("--output_topic projects/${ReleaseConfiguration.PROJECT_ID}/topics/${ReleaseConfiguration.PUBSUB_TOPIC2} ")
    .append("--staging_location gs://${ReleaseConfiguration.BUCKET_NAME}${ReleaseConfiguration.TEMP_DIR} ")
    .append("--temp_location gs://${ReleaseConfiguration.BUCKET_NAME}${ReleaseConfiguration.TEMP_DIR} ")
    .append("--num_workers ${ReleaseConfiguration.NUM_WORKERS} ")
    .append("--sdk_location dist/apache-beam-${ReleaseConfiguration.VERSION}.tar.gz ")

print_separator("Running Streaming wordcount example with DirectRunner with command: ", cmd.toString())
def streaming_wordcount_dataflow_thread = Thread.start(){
  t.run(cmd.toString())
}
t.run("sleep 15")

// verify result
run_pubsub_publish(t)
run_pubsub_pull(t)
t.see("like: 1")
streaming_wordcount_dataflow_thread.stop()

// clean up pubsub topics and subscription
cleanup_pubsub(t)

println '*********************************'
println 'Verification Complete'
println '*********************************'
t.done()





private void run_pubsub_publish(TestScripts t){
    def words = ["hello world!", "I like cats!", "Python", "hello Python", "hello Python"]
    words.each {
      t.run("gcloud alpha pubsub topics publish ${ReleaseConfiguration.PUBSUB_TOPIC1} \"${it}\"")
  }
    t.run("sleep 15")
}

private void run_pubsub_pull(TestScripts t){
    t.run("gcloud alpha pubsub subscriptions pull --project=${ReleaseConfiguration.PROJECT_ID} ${ReleaseConfiguration.PUBSUB_SUBSCRIPTION} --limit=100 --auto-ack")
}
private void create_pubsub(TestScripts t){
    t.run("gcloud alpha pubsub topics create --project=${ReleaseConfiguration.PROJECT_ID} ${ReleaseConfiguration.PUBSUB_TOPIC1}")
    t.run("gcloud alpha pubsub topics create --project=${ReleaseConfiguration.PROJECT_ID} ${ReleaseConfiguration.PUBSUB_TOPIC2}")
    t.run("gcloud alpha pubsub subscriptions create --project=${ReleaseConfiguration.PROJECT_ID} ${ReleaseConfiguration.PUBSUB_SUBSCRIPTION} --topic ${ReleaseConfiguration.PUBSUB_TOPIC2}")
}

private void cleanup_pubsub(TestScripts t){
    t.run("gcloud alpha pubsub topics delete --project=${ReleaseConfiguration.PROJECT_ID} ${ReleaseConfiguration.PUBSUB_TOPIC1}")
    t.run("gcloud alpha pubsub topics delete --project=${ReleaseConfiguration.PROJECT_ID} ${ReleaseConfiguration.PUBSUB_TOPIC2}")
    t.run("gcloud alpha pubsub subscriptions delete --project=${ReleaseConfiguration.PROJECT_ID} ${ReleaseConfiguration.PUBSUB_SUBSCRIPTION}")
}

private void print_separator(String description, String cmd=''){
    println("----------------------------------------------------------------")
    println(description)
    if(cmd.length() > 0){
    	println(cmd.toString())
    }
    println("----------------------------------------------------------------")
}
