<!--
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

## Tour Of Beam Backend

Backend provides the learning content tree for a given SDK,
and currently logged-in user's snippets and progress.
Currently it supports Java, Python, and Go Beam SDK.

It is comprised of several Cloud Functions, with Firerstore in Datastore mode as a storage.
Public endpoints:

* getSdkList
* getContentTree?sdk=(java|go|python)
* getUnitContent?sdk=<sdk>&id=<id>

Authorized endpoints also consume `Authorization: Bearer <id_token>` header

* getUserProgress?sdk=<sdk>
* postUnitComplete?sdk=<sdk>&id=<id>
* postUserCode?sdk=<sdk>&id=<id>

### Playground GRPC API

We use Playground GRPC to save/get user snippets, so we keep the generated stubs in [playground_api](playground_api)
To re-generate, refer to [Playground Readme](../../../playground/README.md), section "Re-generate protobuf".

To update mocks for tests, run:
```
$ go generate -x ./...
```

> Note: [`moq`](https://github.com/matryer/moq) tool to be installed

### Datastore schema

The storage is shared with Beam Playground backend, so that Tour Of Beam could access its entities in
pg_examples and pg_snippets.

Conceptually, the learning tree is a tree with a root node in tb_learning_path,
having several children of tb_learning_module, and each module has its descendant nodes.
Node is either a group or a unit.

Every module or unit has SDK-wide unique ID, which is provided by a content maintainer.
User's progress on a unit is tied to its ID, and if ID changes, the progress is lost.

__Kinds__
- tb_learning_path

  key: `(SDK_JAVA|SDK_PYTHON|SDK_GO)`

- tb_learning_module

  key: `<SDK>_<moduleID>`

  parentKey: Learning Path key SDK

- tb_learning_node

  key: `<SDK>_<persistentID>`

  parentKey: parent module/group key

- tb_user

  key: `uid` from IDToken

- tb_user_progress

  key: `<SDK>_<unitID>`

  parentKey: tb_user entity key


### Deployment
Prerequisites:
 - GCP project with enabled
    * Billing API
    * Cloud Functions API
    * Firebase Admin API
    * Secret Manager API
 - existing setup of Playground backend in a project
   * (output) GKE_CLUSTER_NAME (`playground` by default)
   * (output) GKE_ZONE, like `us-east1-b`
 - set environment variables:
   * PROJECT_ID: GCP id
   * REGION: the region, "us-central1" fe
   * DATASTORE_NAMESPACE: datastore namespace, "Playground" namespace will be used if variable is not set
   * PLAYGROUND_ROUTER_HOST: router serving Playground Router GRPC API

__To discover Router host:__
```
# setup kubectl credentials
gcloud container clusters get-credentials $GKE_CLUSTER_NAME --zone $GKE_ZONE --project $PROJECT_ID

# get external host:port of a backend-router-grpc service
PLAYGROUND_ROUTER_HOST=$(kubectl get svc -l "app=backend-router-grpc" \
    -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}:{.items[0].spec.ports[0].port}'\
)
```

1. Deploy Datastore indexes (but don't delete existing Playground indexes!)
```
gcloud datastore indexes create ./internal/storage/index.yaml
```

2. Deploy cloud functions
```
for endpoint in getSdkList getContentTree getUnitContent getUserProgress postUnitComplete postUserCode; do
gcloud functions deploy $endpoint --entry-point $endpoint \
  --region $REGION --runtime go116 --allow-unauthenticated \
  --trigger-http \
  --set-env-vars="DATASTORE_PROJECT_ID=$PROJECT_ID,DATASTORE_NAMESPACE=$DATASTORE_NAMESPACE,GOOGLE_PROJECT_ID=$PROJECT_ID,PLAYGROUND_ROUTER_HOST=$PLAYGROUND_ROUTER_HOST"
done

```
3. Set environment variables:
- TOB_MOCK: set to 1 to deliver mock responses from samples/api
- DATASTORE_PROJECT_ID: Google Cloud PROJECT_ID
- DATASTORE_NAMESPACE: Datastore namespace to use
- GOOGLE_PROJECT_ID: Google Cloud PROJECT_ID (consumed by Firebase Admin SDK)
- GOOGLE_APPLICATION_CREDENTIALS: path to json auth key
- TOB_LEARNING_ROOT: path the content tree root

4. Populate datastore
```
$ go run cmd/ci_cd/ci_cd.go
```

## Sample usage

Entry point: list sdk names
```
$ curl -X GET "https://$REGION-$PROJECT_ID.cloudfunctions.net/getSdkList" | json_pp
```
[response](./samples/api/get_sdk_list.json)

### Get content tree by sdk name (SDK name == SDK id)
```
$ curl -X GET "https://$REGION-$PROJECT_ID.cloudfunctions.net/getContentTree?sdk=python"
```
[response](./samples/api/get_content_tree.json)


### Get unit content by sdk name and unitId
```
$ curl -X GET "https://$REGION-$PROJECT_ID.cloudfunctions.net/getUnitContent?sdk=python&id=challenge1"
```
[response](./samples/api/get_unit_content.json)

### Get user progress by sdk name
```
$ curl -X GET -H "Authorization: Bearer $token" \
  "https://$REGION-$PROJECT_ID.cloudfunctions.net/getUserProgress?sdk=python"
```
[response](./samples/api/get_user_progress.json)

### Set unit as complete
```
$ curl -X POST -H "Authorization: Bearer $token" \
  "https://$REGION-$PROJECT_ID.cloudfunctions.net/postUnitComplete?sdk=python&id=challenge1" -d '{}'
```

### Save user code
request body:
```json
{
  "pipelineOptions": "some pipeline opts",
  "files": [
    {"name": "main.py", "content": "import sys; sys.exit(0)", "isMain": true}
  ]
}
```

```
$ curl -X POST -H "Authorization: Bearer $token" \
  "https://$REGION-$PROJECT_ID.cloudfunctions.net/postUserCode?sdk=python&id=challenge1" -d @request.json
```

### Delete user progress
```
$ curl -X POST -H "Authorization: Bearer $token" \
  "https://$REGION-$PROJECT_ID.cloudfunctions.net/postDeleteProgress" -d '{}'
```