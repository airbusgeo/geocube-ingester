package log

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type execOption struct {
	outl, errl zapcore.Level
	outf, errf Filter
}

// ExecOption is an option that can be passed to Exec()
type ExecOption func(eo *execOption)

// StdoutLevel sets the level at which stdout should be logged
func StdoutLevel(l zapcore.Level) ExecOption {
	return func(eo *execOption) {
		eo.outl = l
	}
}

// StderrLevel sets the level at which stderr should be logged
func StderrLevel(l zapcore.Level) ExecOption {
	return func(eo *execOption) {
		eo.errl = l
	}
}

// Filter receives a message and the default level and returns a modified message with a new level
// if the last result is true, the msg is ignored
type Filter interface {
	Filter(msg string, defaultLevel zapcore.Level) (string, zapcore.Level, bool)
}

// StdoutFilter sets a function that modify a stdout message or change its level
func StdoutFilter(f Filter) ExecOption {
	return func(eo *execOption) {
		eo.outf = f
	}
}

// StderrFilter sets a function that modify a stderr message or change its level
func StderrFilter(f Filter) ExecOption {
	return func(eo *execOption) {
		eo.errf = f
	}
}

// Exec wraps os/exec for logging its outputs.
// If cmd.Stdout is not set, the commands stdout will
// be sent to log.Logger(ctx) (at Info level by default).
// If cmd.Stderr is not set, the commands
// stderr will be sent to log.Logger(ctx) (at Warn level by default).
// On ctx cancellation, the cmd is Killed
func Exec(ctx context.Context, cmd *exec.Cmd, options ...ExecOption) error {

	opts := execOption{
		outl: zapcore.InfoLevel,
		errl: zapcore.WarnLevel,
	}
	for _, eo := range options {
		eo(&opts)
	}

	logger := Logger(ctx)
	var lout, lerr *levelledLogger
	var stdout, stderr io.Reader
	var err error

	if cmd.Stdout == nil {
		lout = &levelledLogger{logger, opts.outl, opts.outf}
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return fmt.Errorf("get stdout pipe: %w", err)
		}
	}
	if cmd.Stderr == nil {
		lerr = &levelledLogger{logger, opts.errl, opts.errf}
		stderr, err = cmd.StderrPipe()
		if err != nil {
			return fmt.Errorf("get stderr pipe: %w", err)
		}
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cmd.start: %w", err)
	}

	logwg := sync.WaitGroup{}
	if lout != nil {
		logwg.Add(1)
		go func() {
			defer logwg.Done()
			logLines(stdout, lout)
		}()
	}
	if lerr != nil {
		logwg.Add(1)
		go func() {
			defer logwg.Done()
			logLines(stderr, lerr)
		}()
	}

	done := make(chan error, 1)
	go func() {
		//wait for stdout/stderr to be logged
		logwg.Wait()
		done <- cmd.Wait()
	}()

	contextDone := false
	ectx := ctx
	for {
		select {
		case <-ectx.Done():
			contextDone = true
			err := cmd.Process.Kill()
			if err != nil {
				logger.Sugar().Warnf("kill: %v", err)
				return ectx.Err()
			}
			ectx = context.Background()
			//exit will be handled via done channel
		case err := <-done:
			if contextDone {
				return ctx.Err()
			}
			return err
		}
	}
}

// sendLines
func logLines(sr io.Reader, logger *levelledLogger) {
	r := bufio.NewReader(sr)
	insideTooLongLine := false
	for {
		line, err := r.ReadSlice('\n')
		if err == io.EOF {
			if !insideTooLongLine && len(line) > 0 {
				logger.Print(string(line))
			}
			return
		}
		if insideTooLongLine {
			if err == nil {
				//reset
				insideTooLongLine = false
			}
		} else {
			if err == bufio.ErrBufferFull {
				logger.Print(fmt.Sprintf("%s ...[Message clipped]", line))
				insideTooLongLine = true
			} else {
				if len(line) > 0 {
					logger.Print(string(line))
				}
			}
		}
	}
}

type levelledLogger struct {
	*zap.Logger
	level  zapcore.Level
	filter Filter
}

func (l levelledLogger) Print(msg string) {
	level := l.level
	if l.filter != nil {
		var ignore bool
		if msg, level, ignore = l.filter.Filter(msg, level); ignore {
			return
		}
	}

	switch level {
	case zapcore.DebugLevel:
		l.Debug(msg)
	case zapcore.InfoLevel:
		l.Info(msg)
	case zapcore.WarnLevel:
		l.Warn(msg)
	case zapcore.ErrorLevel:
		l.Error(msg)
	case zapcore.DPanicLevel:
		l.DPanic(msg)
	case zapcore.PanicLevel:
		l.Panic(msg)
	case zapcore.FatalLevel:
		l.Fatal(msg)
	}
}
