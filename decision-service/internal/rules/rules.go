package rules

import "github.com/ComderCK12/Sentinel/shared"

const (
	DecisionAllow = "allow"
	DecisionBlock = "block"
)

const blockThreshold = 10000.0

func Evaluate(e *shared.Event) (decission string, reason string) {
	if e.Amount > blockThreshold {
		return DecisionBlock, "amount exceeds dummy threshold"
	}
	return DecisionAllow, "within dummy threshold"
}
