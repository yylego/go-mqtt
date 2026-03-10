package utils_test

import (
	"testing"

	"github.com/yylego/go-mqtt/internal/utils"
)

func TestNewUUID(t *testing.T) {
	t.Log(utils.NewUUID())
}
