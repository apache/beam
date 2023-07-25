To regenerate these files run

pip install google-apitools[cli]
gen_client \
    --discovery_url dataflow.v1b3 \
    --overwrite \
    --root_package=. \
    --outdir=apache_beam/runners/dataflow/internal/clients/dataflow \
    client

Patch up the imports in __init__ to make them conditional.
