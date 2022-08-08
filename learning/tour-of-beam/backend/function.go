package tob

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"beam.apache.org/learning/tour-of-beam/backend/internal/service"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	"cloud.google.com/go/datastore"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var svc service.IContent

func init() {
	// dependencies
	// required:
	// * TOB_MOCK: respond with static samples
	// OR
	// * DATASTORE_PROJECT_ID: cloud project id
	// optional:
	// * DATASTORE_EMULATOR_HOST: emulator host/port (ex. 0.0.0.0:8888)
	if os.Getenv("TOB_MOCK") > "" {
		svc = &service.Mock{}
	} else {
		client, err := datastore.NewClient(context.Background(), "")
		if err != nil {
			log.Fatalf("new datastore client: %v", err)
		}
		svc = &service.Svc{Repo: &storage.DatastoreDb{Client: client}}
	}

	// functions framework
	functions.HTTP("sdkList", sdkList)
	functions.HTTP("getContentTree", getContentTree)
	//functions.HTTP("getUnitContent", getUnitContent)
}

func finalizeErrResponse(w http.ResponseWriter, status int, code, message string) {
	w.WriteHeader(status)
	resp := tob.CodeMessage{Code: code, Message: message}
	_ = json.NewEncoder(w).Encode(resp)
}

func sdkList(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, `{"names": ["Java", "Python", "Go"]}`)
}

func getContentTree(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	sdkStr := r.URL.Query().Get("sdk")
	sdk := tob.FromString(sdkStr)
	if sdk == tob.SDK_UNDEFINED {
		finalizeErrResponse(w, http.StatusBadRequest, "BAD_FORMAT", fmt.Sprintf("Bad sdk: %v", sdkStr))
		return
	}

	tree, err := svc.GetContentTree(context.Background(), sdk, nil /*TODO userId*/)
	if err != nil {
		finalizeErrResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(tree)
}
