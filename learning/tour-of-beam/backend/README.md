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
* list-sdks
* get-content-tree?sdk=(Java|Go|Python)
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
 - set environment PROJECT_ID var


```
$ gcloud functions deploy sdkList --entry-point sdkList \
  --region us-central1 --runtime go116 --allow-unauthenticated \
  --trigger-http --set-env-vars="DATASTORE_PROJECT_ID=$PROJECT_ID"

$ gcloud functions deploy getContentTree --entry-point getContentTree \
  --region us-central1 --runtime go116 --allow-unauthenticated \
  --trigger-http --set-env-vars="DATASTORE_PROJECT_ID=$PROJECT_ID"

$ gcloud functions deploy getUnitContent --entry-point getUnitContent \
  --region us-central1 --runtime go116 --allow-unauthenticated \
  --trigger-http --set-env-vars="DATASTORE_PROJECT_ID=$PROJECT_ID"
```

Environment variables:
- TOB_MOCK: set to 1 to deliver mock responses from samples/api
- DATASTORE_PROJECT_ID: Google Cloud PROJECT_ID

### Sample usage

Entry point: list sdk names
```
$ curl -X GET https://us-central1-$PROJECT_ID.cloudfunctions.net/sdkList | json_pp
{
   "names" : [
      "Java",
      "Python",
      "Go"
   ]
}
```

Get content tree by sdk name (SDK name == SDK id)
```
$ curl -X GET 'https://us-central1-$PROJECT_ID.cloudfunctions.net/getContentTree?sdk=Python'
{
   "modules" : [
      {
         "complexity" : "BASIC",
         "moduleId" : "module1",
         "name" : "Module One",
         "nodes" : [
            {
               "type" : "unit",
               "unit" : {
                  "name" : "Intro Unit Name",
                  "unitId" : "intro-unit"
               }
            },
            {
               "group" : {
                  "name" : "The Group",
                  "nodes" : [
                     {
                        "type" : "unit",
                        "unit" : {
                           "name" : "Example Unit Name",
                           "unitId" : "example1"
                        }
                     },
                     {
                        "type" : "unit",
                        "unit" : {
                           "name" : "Challenge Name",
                           "unitId" : "challenge1"
                        }
                     }
                  ]
               },
               "type" : "group"
            }
         ]
      }
   ],
   "sdk" : "Python"
}
```


Get unit content tree by sdk name and unitId
```
$ curl -X GET 'https://us-central1-$PROJECT_ID.cloudfunctions.net/getContentTree?sdk=Python&unitId=challenge1'
{
   "description" : "## Challenge description\n\nawesome description\n",
   "hints" : [
      "## Hint 1\n\nhint 1",
      "## Hint 2\n\nhint 2"
   ],
   "name" : "Challenge Name",
   "unitId" : "challenge1"
}

```