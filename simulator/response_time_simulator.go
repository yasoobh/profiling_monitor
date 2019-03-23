package simulator

import ("gonum.org/v1/gonum/stat/distuv"
)

// Assume 1 minute windows for ResponseTime
type ResponseTimeSimulator struct {
	MorningResponseTimeAmplitude float64
	DayResponseTimeAmplitude float64
	NightResponseTimeAmplitude float64
}

// 0 <= time < 24
func (rts ResponseTimeSimulator) getMorningCurveResponseTime(time float64) float64 {
	dist := distuv.Normal{
	    Mu:    2,
	    Sigma: 5,
	}

	return dist.Prob(time)*rts.MorningResponseTimeAmplitude
}

// 0 <= time < 24
func (rts ResponseTimeSimulator) getDayCurveResponseTime(time float64) float64 {
	dist := distuv.Normal{
	    Mu:    13,
	    Sigma: 4,
	}

	return dist.Prob(time)*rts.DayResponseTimeAmplitude
}

// 0 <= time < 24
func (rts ResponseTimeSimulator) getNightCurveResponseTime(time float64) float64 {
	dist := distuv.Normal{
	    Mu:    21,
	    Sigma: 2,
	}

	return dist.Prob(time)*rts.NightResponseTimeAmplitude
}

func (rts ResponseTimeSimulator) GetResponseTime(time float64) float64 {
	return rts.getMorningCurveResponseTime(time) + rts.getDayCurveResponseTime(time) + rts.getNightCurveResponseTime(time)
}



