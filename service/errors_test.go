package service

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestRetriable(t *testing.T) {
	i := 0
	ctx := context.Background()
	tim := time.Now()
	err := Retriable(ctx, func() error {
		i++
		return fmt.Errorf("%d", i)
	}, time.Microsecond, 3)

	if time.Since(tim) < 3*time.Microsecond {
		t.Errorf("err: excepted at least 30µs got %v", time.Since(tim))
	}

	if err == nil {
		t.Error("err: excepted 3 got nil")
	}
	if err.Error() != "3" {
		t.Error("err: excepted 3 got " + err.Error())
	}

}
