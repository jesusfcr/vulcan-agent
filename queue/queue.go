package queue

import (
	"context"
	"errors"
	"time"
)

var (
	// ReaderStopperPollPeriod defines the time period a ReaderStopper will get the
	// last time a Reader received a message.
	ReaderStopperPollPeriod = 5

	// ErrMaxTimeNoMessageExceeded is returned in the Track channel when the
	// maximun time without message has been elapsed.
	ErrMaxTimeNoMessageExceeded = errors.New("max time without messages read exceeded")
)

// MessageProcessor defines the methods needed by a queue reader implementation
// to process the messages it reads.
type MessageProcessor interface {
	FreeTokens() chan interface{}
	ProcessMessage(msg string, token interface{}) <-chan bool
}

// Reader defines the functions that all the concrete queue reader
// implementations must fullfil.
type Reader interface {
	StartReading(ctx context.Context) <-chan error
	LastMessageReceived() time.Time
}

// Writer defines the functions that a queue writer must implement.
type Writer interface {
	Write(body string) error
}

// ReaderStopper tracks a the time the last message was received by a queue reader.
// If that time is greater than the specified period, the ReaderStopper calls the passed
// in function.
type ReaderStopper struct {
	R       Reader
	MaxTime time.Duration
}

// Track reads the LastMessageReceived in the Reader stored in the receiver and
// if it exceeds the period duration, a call to the passed in cancel function is
// done and the Track goroutine ends by calling the returned channel. The track
// goroutine also ends if the passed in context is canceled.
func (rs ReaderStopper) Track(ctx context.Context) <-chan error {
	pollTime := time.Duration(ReaderStopperPollPeriod) * time.Second
	pollTimer := time.NewTimer(pollTime)
	var done = make(chan error, 1)
	go func() {
		var err error
		defer func() {
			done <- err
			close(done)
		}()
	loop:
		for {
			select {
			case <-pollTimer.C:
				last := rs.R.LastMessageReceived()
				now := time.Now()
				if now.Sub(last) > rs.MaxTime {
					break loop
				}
				pollTimer.Reset(pollTime)
			case <-ctx.Done():
				err = ctx.Err()
				break loop
			}
		}
	}()
	return done
}
