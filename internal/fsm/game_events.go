package fsm

type Decision int

const (
	Unknown Decision = iota
	Cooperate
	Defect
)

func (d Decision) String() string {
	switch d {
	case Cooperate:
		return "cooperate"
	case Defect:
		return "defect"
	default:
		return "unknown"
	}
}

// Strategy identifies a participant in the tournament. A strategy is the unit a
// game is played between; how many replicas back it is a deployment detail that
// the game rules never see (that is what platform.Header.SenderId is for).
type Strategy string

const (
	// Cooperator always cooperates. Depends on nothing: no coordination.
	Cooperator Strategy = "cooperator"
	// Flipper always flips its own previous decision. Depends on private
	// per-strategy state: no coordination with other strategies.
	Flipper Strategy = "flipper"
	// Retaliator retaliates once with defect if the opponent defected. Depends on
	// this specific opponent's last move: coordination within the pair only.
	Retaliator Strategy = "retaliator"
	// CopyLeader copies the last decision of the current leading strategy.
	// Depends on global shared state: the one place real blocking is required.
	CopyLeader Strategy = "copy-leader"
)

// AllStrategies is the canonical, deterministically ordered strategy set. Never
// iterate a map to enumerate strategies: Go randomizes map iteration order, which
// is exactly the class of hidden nondeterminism invariant 3 forbids.
var AllStrategies = []Strategy{Cooperator, Flipper, Retaliator, CopyLeader}

type NewGame struct {
	Id        int64
	StrategyA Strategy
	StrategyB Strategy
}

type GameDecision struct {
	Strategy Strategy
	Decision Decision
}
