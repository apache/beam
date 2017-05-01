package reflectx

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// UnmarshalJSON decodes a json string as then given type. It is suitable
// for when the type is not statically known.
func UnmarshalJSON(t reflect.Type, str string) (interface{}, error) {
	data := reflect.New(t).Interface()
	if err := json.Unmarshal([]byte(str), data); err != nil {
		return nil, fmt.Errorf("Failed to decode data: %v", err)
	}
	return reflect.ValueOf(data).Elem().Interface(), nil
}
