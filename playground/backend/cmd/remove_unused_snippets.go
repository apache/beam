package main

import (
	"beam.apache.org/playground/backend/internal/db/datastore"
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	dayDiff := os.Args[1]
	projectId := os.Args[2]
	fmt.Println("Removing unused snippets is running...")
	startDate := time.Now()

	ctx := context.Background()
	db, err := datastore.New(ctx, projectId)
	if err != nil {
		fmt.Printf("Couldn't create the database client, err: %s \n", err.Error())
		return
	}

	diff, err := strconv.Atoi(dayDiff)
	if err != nil {
		fmt.Printf("Couldn't convert days to integer from the input parameter, err: %s \n", err.Error())
		return
	}

	err = db.DeleteUnusedSnippets(ctx, int32(diff))
	if err != nil {
		fmt.Printf("Couldn't delete unused code snippets, err: %s \n", err.Error())
		return
	}

	diffTime := time.Now().Sub(startDate).Milliseconds()
	fmt.Printf("Removing unused snippets finished, work time: %d ms\n", diffTime)
}
