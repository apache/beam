<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# The Tour of Beam deployment on GCP
This guide provides instructions on how to deploy the Tour of Beam environment on Google Cloud Platform (GCP) and Firebase environment.
Before starting the deployment, ensure that you have the following prerequisites in place:

## Prerequisites:

1. [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
2. [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
   Ensure that the account has at least following privileges:
   - Cloud Datastore Owner
   - Create Service Accounts
   - Security Admin
   - Service Account User
   - Service Usage Admin
   - Storage Admin
   - Kubernetes Engine Cluster Viewer
   - Firebase Admin
   - Cloud Functions Admin


3. [Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) for saving deployment state

4. An OS with the following software installed:

* [Flutter (3.7.3 >)](https://docs.flutter.dev/get-started/install)
* [Dart SDK (2.19.2)](https://dart.dev/get-dart)
* [Firebase-tools CLI](https://www.npmjs.com/package/firebase-tools)
* [Terraform](https://www.terraform.io/downloads)
* [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
* [Kubectl authentication plugin](https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke)

5. Existing Beam Playground environment

6. Apache Beam Git repository cloned locally

# Prepare deployment configuration:


1. Navigate to Apache Beam cloned repository's `beam/learning/tour-of-beam/terraform` directory

```
cd beam/learning/tour-of-beam/terraform
```

2. Configure authentication for the Google Cloud Platform (GCP). _(Note: Authentication to the GCP Project required to run gcloud commands)_<br>

```
gcloud init

gcloud auth application-default login
```

3. Configure authentication in the GCP Docker registry:
```
 gcloud auth configure-docker `chosen_region`-docker.pkg.dev
```

4. And the authentication in GCP Google Kubernetes Engine: _(Note: Authentication to docker and GKE required to fetch GRPC router ip:port info)_<br>
```
gcloud container clusters get-credentials --region `chosen_gke_zone` `gke_name` --project `project_id`
```

5. Create datastore indexes:
```
gcloud datastore indexes create ../backend/internal/storage/index.yaml
```

# Deploy the Tour of Beam Backend Infrastructure:

6. Initialize terraform
```
terraform init -backend-config="bucket=`created_gcs_bucket`"
```

7. Run terraform apply to create Tour-Of-Beam backend infrastructure

```
terraform plan -var "environment=prod" \
-var "region=us-west1" \
-var "project_id=$(gcloud config get-value project)" \
-var "datastore_namespace=playground-datastore-namespace" \
-var "pg_router_host=$(kubectl get svc -l app=backend-router-grpc -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}:{.items[0].spec.ports[0].port}')"
```

```
terraform apply -var "environment=prod" \
-var "region=us-west1" \
-var "project_id=$(gcloud config get-value project)" \
-var "datastore_namespace=playground-datastore-namespace" \
-var "pg_router_host=$(kubectl get svc -l app=backend-router-grpc -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}:{.items[0].spec.ports[0].port}')"
```

Where:
- **environment** - Infrastructure environment name
- **region** - GCP region for your infrastructure
- **datastore_namespace** - Beam Playground Datastore's namespace

# Deploy the Tour of Beam Frontend Infrastructure:

8. Update config.dart configuration file under beam/learning/tour-of-beam/frontend/lib:

   8.1. Navigate to beam/learning/tour-of-beam/frontend/lib.

   8.2. Update config.dart file, replacing values in ${ } with your actual values.

Where:
- **${cloudfunctions_region}** - region where GCP Cloud Functions have been deployed
- **${project_id}** - GCP project where infrastructure being deployed
- **${environment}** - Infrastructure environment name
- **${dns_name}** - DNS record name reserved for Beam Playground environment

```
const _cloudFunctionsProjectRegion = '${cloudfunctions_region}';
const _cloudFunctionsProjectId = '${project_id}';
const cloudFunctionsBaseUrl = 'https://'
   '$_cloudFunctionsProjectRegion-$_cloudFunctionsProjectId'
   '.cloudfunctions.net/${environment}_';

9. Create file .firebaserc under beam/learning/tour-of-beam/frontend

   9.1. Navigate to beam/learning/tour-of-beam/frontend.

   9.2. Create .firebaserc file with the following content.

Where:
- **${project_id}** - GCP project where infrastructure being deployed

```
{
"projects": {
"default": "${project_id}"
   }
}
```

10. Login into the Firebase CLI

```
# To use an interactive mode (forwards to a browser webpage)
firebase login
```

```
# To use non-interactive mode (generates link)
firebase login --no-localhost
```


11. Create Firebase Project

```
firebase projects:addfirebase
```

12. Create Firebase Web App and prepare Firebase configuration file

```
firebase apps:create WEB ${webapp_name} --project=$(gcloud config get-value project)
```

Once Firebase Web App has been created, there will be following output example:

```
Create your WEB app in project cloudbuild-383310:
✔ Creating your Web app

🎉🎉🎉 Your Firebase WEB App is ready! 🎉🎉🎉

App information:
- App ID: WEBAPP_ID
- Display name: WEBAPP_NAME

You can run this command to print out your new app's Google Services config:
firebase apps:sdkconfig WEB WEBAPP_ID
```

Copy and paste into the terminal last line to get Web App configuration.

Output example:

```
✔ Downloading configuration data of your Firebase WEB app
// Copy and paste this into your JavaScript code to initialize the Firebase SDK.
// You will also need to load the Firebase SDK.
// See https://firebase.google.com/docs/web/setup for more details.

firebase.initializeApp({
  "projectId": "cloudbuild-384304",
  "appId": "1:1111111111:web:111111111111",
  "storageBucket": "cloudbuild-384304.appspot.com",
  "locationId": "us-west1",
  "apiKey": "someApiKey",
  "authDomain": "cloudbuild-384304.firebaseapp.com",
  "messagingSenderId": "111111111111"
});
```

Copy the lines inside the curly braces and redact them.

You will need to:

1) Remove "locationId" line.
2) Remove quotes (") from key of "key": "value" pair.
3) E.g. `projectId: "cloudbuild-384304"`
4) In overall, redacted and ready to be inserted data should be as follows:

```
   projectId: "cloudbuild-384304",
   appId: "1:1111111111:web:111111111111",
   storageBucket: "cloudbuild-384304.appspot.com",
   apiKey: "someApiKey",
   authDomain: "cloudbuild-384304.firebaseapp.com",
   messagingSenderId: "111111111111"
```

Paste (replace) the redacted data inside the parentheses in beam/learning/tour-of-beam/frontend/lib/firebase_options.dart file.

```
static const FirebaseOptions web = FirebaseOptions(


);
```

13. Run flutter and firebase commands to deploy Tour of Beam frontend

Navigate to beam/playground/frontend/playground_components and run flutter commands

```
# Go to beam/playground/frontend/playground_components first
flutter pub get
flutter pub run build_runner build --delete-conflicting-outputs
```

Navigate to beam/learning/tour-of-beam/frontend and run flutter commands

```
# Go to beam/learning/tour-of-beam/frontend first
flutter pub get
flutter pub run build_runner build --delete-conflicting-outputs
flutter build web --profile --dart-define=Dart2jsOptimization=O0
firebase deploy --project=$(gcloud config get-value project)
```

# Validate the deployment of the Tour of Beam:

14. Open the Tour of Beam webpage in a web browser (Hosting URL will be provided in terminal output) to ensure that deployment has been successfully completed.

Example:
```
✔  Deploy complete!

Project Console: https://console.firebase.google.com/project/some-gcp-project-id/overview
Hosting URL: https://some-gcp-project-id.web.app
```