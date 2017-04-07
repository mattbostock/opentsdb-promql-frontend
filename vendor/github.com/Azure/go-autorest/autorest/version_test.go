package autorest

import (
	"testing"
)

func TestVersion(t *testing.T) {
	v := "5.0.0"
	if Version() != v {
		t.Errorf("autorest: Version failed to return the expected version -- expected %s, received %s",
			v, Version())
	}
}
