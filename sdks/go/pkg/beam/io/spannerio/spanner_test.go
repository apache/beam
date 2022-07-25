package spannerio

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type TestDto struct {
	One string `spanner:"one"`
	Two int    `spanner:"two"`
}

func TestColumnsFromStructReturnsColumns(t *testing.T) {
	// arrange
	// act
	cols := columnsFromStruct(reflect.TypeOf(TestDto{}))

	// assert
	assert.NotEmpty(t, cols)
	assert.Equal(t, "one", cols[0])
	assert.Equal(t, "two", cols[1])
}
