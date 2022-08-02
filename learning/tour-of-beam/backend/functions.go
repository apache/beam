package function

import (
	"encoding/json"
	"fmt"
	"net/http"

	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

func init() {
	functions.HTTP("sdkList", sdkList)
	functions.HTTP("getContentTree", getContentTree)
}

func sdkList(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, `{"sdk": ["Python", "Java", "Go"]}`)
}

func getContentTree(w http.ResponseWriter, r *http.Request) {
	// if r.Method != "GET" {
	// 	w.WriteHeader(http.StatusMethodNotAllowed)
	// 	return
	// }

	mock := storage.Mock{}
	ct := mock.GetContentTree("Java", nil)
	_ = json.NewEncoder(w).Encode(ct)
}
