package utils

import (
	"reflect"
	"runtime"
	"strings"
)

// GetFuncName returns the name of the received func
func GetFuncName(i interface{}) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	splitName := strings.Split(fullName, ".")
	return splitName[len(splitName)-1]
}
