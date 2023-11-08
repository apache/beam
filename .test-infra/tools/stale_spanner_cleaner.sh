set -euo pipefail

PROJECT=apache-beam-testing

# Delete spanner databases older than 1 day.
gcloud spanner databases list \
--instance beam-test \
--project $PROJECT \
--filter="createTime < $(date --iso-8601=s -d '1 day ago')" \
--format="value(name)" | \
xargs -I{} gcloud spanner databases delete {} --instance beam-test --quiet
