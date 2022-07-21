package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"beam.apache.org/playground/backend/internal/db/datastore"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Go must have at least three arguments")
		return
	}
	dayDiff := os.Args[1]
	projectId := os.Args[2]
	fmt.Println("Removing unused snippets is running...")
	startDate := time.Now()

	diff, err := strconv.Atoi(dayDiff)
	if err != nil {
		fmt.Printf("Couldn't convert days to integer from the input parameter, err: %s \n", err.Error())
		return
	}

	ctx := context.Background()
	db, err := datastore.New(ctx, projectId)
	if err != nil {
		fmt.Printf("Couldn't create the database client, err: %s \n", err.Error())
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
