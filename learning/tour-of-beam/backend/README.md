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
* get-sdk-list
* get-content-tree?sdk=(java|go|python)
* get-unit-content?unitId=<id>
TODO: add response schemas
TODO: add save functions info
TODO: add user token info

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


### Deployment
Prerequisites:
 - GCP project with enabled Billing API & Cloud Functions API
 - set environment variables:
   * PROJECT_ID: GCP id
   * REGION: the region, "us-central1" fe
 - existing setup of Playground backend in a project

1. Deploy Datastore indexes
```
gcloud datastore indexes create ./internal/storage/index.yaml
```

2. Deploy cloud functions
```
$ gcloud functions deploy getSdkList --entry-point getSdkList \
  --region $REGION --runtime go116 --allow-unauthenticated \
  --trigger-http --set-env-vars="DATASTORE_PROJECT_ID=$PROJECT_ID"

$ gcloud functions deploy getContentTree --entry-point getContentTree \
  --region $REGION --runtime go116 --allow-unauthenticated \
  --trigger-http --set-env-vars="DATASTORE_PROJECT_ID=$PROJECT_ID"

$ gcloud functions deploy getUnitContent --entry-point getUnitContent \
  --region $REGION --runtime go116 --allow-unauthenticated \
  --trigger-http --set-env-vars="DATASTORE_PROJECT_ID=$PROJECT_ID"
```
3. Set environment variables:
- TOB_MOCK: set to 1 to deliver mock responses from samples/api
- DATASTORE_PROJECT_ID: Google Cloud PROJECT_ID
- GOOGLE_APPLICATION_CREDENTIALS: path to json auth key
- TOB_LEARNING_PATH: path the content tree root

4. Populate datastore
```
$ go run cmd/ci_cd/ci_cd.go
```

### Sample usage

Entry point: list sdk names
```
$ curl -X GET https://$REGION-$PROJECT_ID.cloudfunctions.net/getSdkList | json_pp
```
[response](./samples/api/get_sdk_list.json)

Get content tree by sdk name (SDK name == SDK id)
```
$ curl -X GET 'https://$REGION-$PROJECT_ID.cloudfunctions.net/getContentTree?sdk=python'
```
[response](./samples/api/get_content_tree.json)


Get unit content tree by sdk name and unitId
```
$ curl -X GET 'https://$REGION-$PROJECT_ID.cloudfunctions.net/getContentTree?sdk=python&id=challenge1'
```
[response](./samples/api/get_unit_content.json)
