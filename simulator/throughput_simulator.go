package simulator

import ("gonum.org/v1/gonum/stat/distuv"
	"math"
)

// Assume 1 minute windows for throughput
type ThroughputSimulator struct {
	DayThroughputAmplitude float64
	NightThroughputAmplitude float64
}

// 0 <= time < 24
func (ts ThroughputSimulator) getFirstCurveThroughput(time float64) float64 {
	sqrt_seven := math.Sqrt(7)

	dist := distuv.Normal{
	    Mu:    13,
	    Sigma: sqrt_seven,
	}

	return dist.Prob(time)*ts.DayThroughputAmplitude
}

// 0 <= time < 24
func (ts ThroughputSimulator) getSecondCurveThroughput(time float64) float64 {
	sqrt_seven := math.Sqrt(7)

	dist := distuv.Normal{
	    Mu:    21,
	    Sigma: sqrt_seven,
	}

	return dist.Prob(time)*ts.NightThroughputAmplitude
}

func (ts ThroughputSimulator) GetThroughput(time float64) float64 {
	return ts.getFirstCurveThroughput(time) + ts.getSecondCurveThroughput(time)
}
