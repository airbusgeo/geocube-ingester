package common

//go:generate enumer -json -sql -type Status -trimprefix Status

type Status int

const (
	StatusNEW Status = iota
	StatusPENDING
	StatusDONE
	StatusFAILED
	StatusRETRY
)

func (s Status) Color() string {
	switch s {
	case StatusNEW:
		return "gray"
	case StatusPENDING:
		return "blue"
	case StatusRETRY:
		return "orange"
	case StatusDONE:
		return "green"
	case StatusFAILED:
		return "red"
	}
	return "white"
}
