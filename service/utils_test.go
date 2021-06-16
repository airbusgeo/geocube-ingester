package service

import (
	"context"
	"fmt"
	"net/url"
	"testing"
)

func TestPermanent(t *testing.T) {
	err := fmt.Errorf("Permanent error")
	if Temporary(err) {
		t.Fail()
	}
	err = &url.Error{Err: err}
	if Temporary(err) {
		t.Fail()
	}
}

func TestTemporary(t *testing.T) {
	err := MakeTemporary(fmt.Errorf("Temporary error"))
	if !Temporary(err) {
		t.Fail()
	}
	err = fmt.Errorf("Warp: %w", err)
	if !Temporary(err) {
		t.Fail()
	}
	if !Temporary(context.Canceled) {
		t.Fail()
	}
	if !Temporary(context.DeadlineExceeded) {
		t.Fail()
	}
	err = fmt.Errorf("Warp: %w", &url.Error{Err: err})
	if !Temporary(err) {
		t.Fail()
	}
}
