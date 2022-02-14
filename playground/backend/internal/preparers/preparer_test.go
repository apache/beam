package preparers

import (
	"beam.apache.org/playground/backend/internal/logger"
	"os"
	"testing"
)

const (
	correctGoCode   = "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello world\")\n\n\n}\n"
	correctGoFile   = "correct.go"
	incorrectGoCode = "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello world\"\n\n}\n"
	incorrectGoFile = "incorrect.go"
	pyCode          = "import logging as l\n\nif __name__ == \"__main__\":\n    logging.info(\"INFO\")\n"
	correctPyFile   = "original.py"
	incorrectPyFile = "someFile.py"
)

func TestMain(m *testing.M) {
	err := setupPreparedFiles()
	if err != nil {
		logger.Fatal(err)
	}
	defer teardown()
	m.Run()
}

// setupPreparedFiles creates 2 go programs:
// correctGoFile - program without errors
// incorrectGoFile - program with errors
func setupPreparedFiles() error {
	err := createFile(correctGoFile, correctGoCode)
	if err != nil {
		return err
	}
	err = createFile(incorrectGoFile, incorrectGoCode)
	if err != nil {
		return err
	}
	err = createFile(correctPyFile, pyCode)
	if err != nil {
		return err
	}
	return nil
}

//createFile create file with fileName and write text to it
func createFile(fileName, text string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(text))
	if err != nil {
		return err
	}
	return nil
}

func teardown() {
	err := os.Remove(correctGoFile)
	if err != nil {
		logger.Fatal(err)
	}
	err = os.Remove(incorrectGoFile)
	if err != nil {
		logger.Fatal(err)
	}
	err = os.Remove(correctPyFile)
	if err != nil {
		logger.Fatal(err)
	}
}
