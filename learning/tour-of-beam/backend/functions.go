package function

import (
	"encoding/json"
	"fmt"
	"net/http"

	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var db storage.Iface

func init() {
	// dependencies
	db = &storage.Mock{}

	// functions framework
	functions.HTTP("sdkList", sdkList)
	functions.HTTP("getContentTree", getContentTree)
	functions.HTTP("getUnitContent", getUnitContent)
}

func sdkList(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, `{"names": ["Python", "Java", "Go"]}`)
}

func getContentTree(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ct := db.GetContentTree("Java", nil)
	w.Header().Add("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ct)
}

func getUnitContent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	uc := db.GetUnitContent("unit_id_111", nil)
	w.Header().Add("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(uc)
}
