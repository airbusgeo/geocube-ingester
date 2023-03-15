package service

import (
	"context"
	"errors"
	"fmt"
	neturl "net/url"
	"syscall"

	"google.golang.org/api/googleapi"
)

type errTmpIf interface{ Temporary() bool }
type errTmp struct{ error }

func (t errTmp) Temporary() bool    { return true }
func (t *errTmp) Unwrap() error     { return t.error }
func MakeTemporary(err error) error { return &errTmp{err} }

type errFatalIf interface{ Fatal() bool }
type errFatal struct{ error }

func (t errFatal) Fatal() bool    { return true }
func (t *errFatal) Unwrap() error { return t.error }
func MakeFatal(err error) error   { return &errFatal{err} }

// Temporary inspects the error trace and returns whether the error is transient
func Temporary(err error) bool {
	var uerr *neturl.Error
	if errors.As(err, &uerr) {
		err = uerr.Err
	}

	//First override some default syscall temporary statuses
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EIO, syscall.EBUSY, syscall.ECANCELED, syscall.ECONNABORTED, syscall.ECONNRESET, syscall.ENOMEM, syscall.EPIPE:
			return true
		}
	}

	//first check explicitely marked error
	var tmp errTmpIf
	if errors.As(err, &tmp) {
		return tmp.Temporary()
	}
	var gapiError *googleapi.Error
	if errors.As(err, &gapiError) {
		return gapiError.Code == 429 || gapiError.Code == 500
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

// Fatal inspects the error and returns whether it's a fatal error
func Fatal(err error) bool {
	var tmp errFatalIf
	if errors.As(err, &tmp) {
		return tmp.Fatal()
	}
	return false
}

// MergeErrors, appending texts
// if priorityToErr is true, priority to the fatal error then to the temporary
// else, priority to no error, then to the temporary and finally to the fatal error.
func MergeErrors(priorityToError bool, err error, newErrs ...error) error {
	if len(newErrs) == 0 {
		return err
	}
	newErr := newErrs[0]

	if newErr == nil {
		if !priorityToError {
			return nil
		}
	} else if err == nil {
		err = newErr
	} else if priorityToError != Temporary(err) {
		err = fmt.Errorf("%w\n %v", err, newErr)
	} else {
		err = fmt.Errorf("%w\n %v", newErr, err)
	}
	return MergeErrors(priorityToError, err, newErrs[1:]...)
}
