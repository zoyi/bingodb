package util

import "time"

type Time struct {
	time.Time
}

func Now() Time {
	return Time{Time: time.Now().UTC()}
}

func (t Time) Millisecond() int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func NewTime(milliseconds int64) time.Time {
	return time.Unix(0, milliseconds*int64(time.Millisecond))
}
