package main

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	//	"beam.apache.org/learning/tour-of-beam/backend/internal/fs_content"
)

/*
var repo storage.Iface

func init() {
	client, err := datastore.NewClient(context.Background(), "")
	if err != nil {
		log.Panicf("new datastore client: %v", err)
	}
	repo = &storage.DatastoreDb{Client: client}
}
*/
func main() {
	learningRoot := os.Getenv("TOB_LEARNING_ROOT")
	fmt.Printf("Parsing learning-content at %q\n", learningRoot)
	/*
		trees, err := fs_content.CollectLearningTree(learningRoot)
		if err != nil {
			log.Panic(err)
		}

		fmt.Printf("found %v sdks\n", len(trees))
		for _, tree := range trees {
			if err = repo.SaveContentTree(context.Background(), tree); err != nil {
			}

		}
	*/

	err := filepath.WalkDir(learningRoot, func(path string, d fs.DirEntry, err error) error {
		log.Println(path)
		return nil
	})
	log.Println(err)

}
