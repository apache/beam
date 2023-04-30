locals { 
     cloudbuild_init_environment = [ 
        "terraform_version=$_TF_VERSION",
        "pg_region=$_PG_REGION",
        "pg_gke_zone=$_PG_GKE_ZONE",
        "pg_gke_name=$_PG_GKE_NAME",
        "project_id=$PROJECT_ID",
        "state_bucket=$_STATE_BUCKET",
        "env=$_ENV_NAME",
        "tob_region=$_TOB_REGION",
        "pg_datastore_namespace=$_PG_DATASTORE_NAMESPACE"
     ]
}