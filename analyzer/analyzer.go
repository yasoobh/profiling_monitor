package analyzer

import ("fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"math"
	// "time"
	"sync"
)

var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("mysql", "root:ongbak3@tcp(localhost)/profiling_monitor")
	if (err != nil) {
		panic(err.Error())
	}
}

func Release() {
	db.Close()
}

type PeriodAggregateExtended struct {
	PeriodStartTime uint32
	PeriodInMinutes uint32
	MinResponseTime float64
	MaxResponseTime float64
	AverageResponseTime uint32
	TotalCount uint32
	IsNil bool
}

type PeriodAggregate struct {
	MinResponseTime float64
	MaxResponseTime float64
	TotalResponseTime float64
	TotalCount uint32
}

type periodAggregateQueryParams struct {
	StartTimestamp uint32
	EndTimestamp uint32
	Period string
	Seconds uint32
}

type segment struct {
	StartTimestamp uint32
	EndTimestamp uint32
}

func GetAnalysisForPeriod(start_timestamp, end_timestamp uint32) []PeriodAggregateExtended {
	var period_aggregates_ext []PeriodAggregateExtended
	var wg sync.WaitGroup

	// start := time.Now()

	// diff := end_timestamp - start_timestamp
	// fmt.Println("Diff:", diff)

	segments := GetSegmentsForPeriod(start_timestamp, end_timestamp)

	// fmt.Println(segments)

	periodInMinutes := (end_timestamp - start_timestamp)/uint32(len(segments))

	aggregate_chan := make(chan PeriodAggregateExtended, len(segments))

	for _, segment := range(segments) {
		wg.Add(1)
		go Analyze(segment, periodInMinutes, &wg, aggregate_chan)
	}

	wg.Wait()

	for i := 0; i < len(segments); i++ {
		period_aggregates_ext = append(period_aggregates_ext, <-aggregate_chan)
	}

	// t := time.Now()
	// elapsed := t.Sub(start)
	// fmt.Println("Elapsed ms:", elapsed)

	return period_aggregates_ext
}

func GetSegmentsForPeriod(start_timestamp, end_timestamp uint32) []segment {
	var segments []segment

	diff := end_timestamp - start_timestamp
	minutes := diff/60

	var new_start_timestamp, new_end_timestamp uint32

	var i, number_of_segments, seconds uint32
	var seg segment
	var period string

	if minutes <= 1800 {
		switch {
		case minutes <= 100:
			period = "ONE_MINUTE"
		case minutes <= 300:
			period = "THREE_MINUTES"
		case minutes <= 900:
			period = "TEN_MINUTES"
		case minutes <= 1800:
			period = "THIRTY_MINUTES"
		default:
			period = "NIL_PERIOD"
		}

		if period != "NIL_PERIOD" {
			seconds = secondsFromPeriod(period)
			new_start_timestamp = getEndTimestampOfPeriod(start_timestamp, period)
			new_end_timestamp = getStartTimestampOfPeriod(end_timestamp, period)

			number_of_segments = (new_end_timestamp - new_start_timestamp)/seconds
			for i = 0; i<number_of_segments; i++ {
				seg.StartTimestamp = new_start_timestamp + i*seconds
				seg.EndTimestamp = new_start_timestamp + (i+1)*seconds
				segments = append(segments, seg)
			}
		}
	} else {
		number_of_segments = FindIdealNumberOfSegments(minutes)

		segment_size := diff/number_of_segments

		var i, segment_start_timestamp, segment_end_timestamp uint32
		for i = 0; i < number_of_segments - 1; i++ {
			segment_start_timestamp = start_timestamp + i*segment_size
			segment_end_timestamp = start_timestamp + (i+1)*segment_size
			// fmt.Println(segment_start_timestamp, segment_end_timestamp)
			seg.StartTimestamp = segment_start_timestamp
			seg.EndTimestamp = segment_end_timestamp
			segments = append(segments, seg)
		}

		segment_start_timestamp = segment_end_timestamp
		segment_end_timestamp = end_timestamp

		seg.StartTimestamp = segment_start_timestamp
		seg.EndTimestamp = segment_end_timestamp
		segments = append(segments, seg)

		// fmt.Println(segment_start_timestamp, segment_end_timestamp)
	}

	return segments
}

func FindIdealNumberOfSegments(minutes uint32) uint32 {
	var min_number_of_segments uint32 = 30
	var max_number_of_segments uint32 = 60
	
	min_remainder := max_number_of_segments + 1

	var ideal_number_of_segments uint32
	var remainder uint32

	for number_of_segments := min_number_of_segments; number_of_segments <= max_number_of_segments; number_of_segments++ {
		// fmt.Println("---------------")
		// fmt.Println("number of segments:", number_of_segments)
		remainder = minutes%number_of_segments
		// fmt.Println("remainder:", remainder)
		// fmt.Println("min_remainder:", min_remainder)
		if remainder < min_remainder {
			min_remainder = remainder
			ideal_number_of_segments = number_of_segments
		}
		// fmt.Println("ideal seg:", ideal_number_of_segments)
		// fmt.Println("---------------")
	}

	return ideal_number_of_segments
}

func Analyze(seg segment, period uint32, wg *sync.WaitGroup, aggregate_chan chan PeriodAggregateExtended) {
	defer wg.Done()
	
	var period_aggregate_ext PeriodAggregateExtended

	period_aggregate := getPeriodAggregate(seg.StartTimestamp, seg.EndTimestamp)

	period_aggregate_ext.MinResponseTime = period_aggregate.MinResponseTime
	period_aggregate_ext.MaxResponseTime = period_aggregate.MaxResponseTime
	period_aggregate_ext.AverageResponseTime = uint32(math.Floor(period_aggregate.TotalResponseTime/float64(period_aggregate.TotalCount)))
	period_aggregate_ext.TotalCount = period_aggregate.TotalCount
	period_aggregate_ext.PeriodStartTime = getStartTimestampOfPeriod(seg.StartTimestamp, "ONE_MINUTE")
	period_aggregate_ext.PeriodInMinutes = period

	if period_aggregate.MaxResponseTime == 0 {
		period_aggregate_ext.IsNil = true
	} else {
		period_aggregate_ext.IsNil = false
	}
	aggregate_chan <- period_aggregate_ext
}

func getPeriodAggregate(start_timestamp, end_timestamp uint32) PeriodAggregate {
	// diff = end - start
	// find biggest period which is contained in the range
	// go result for those big periods
	// go getPeriodAggregate() for the sub ranges

	// fmt.Println("timestamps: ", start_timestamp, end_timestamp)
	paqp := GetLongestPeriods(start_timestamp, end_timestamp)
	// fmt.Println("paqp:", paqp)

	if (paqp.Period == "NIL_PERIOD") {
		return nilPeriodAggregate()
	}

	period_aggregate := getPeriodAggregateForTimestampRange(paqp)
	// fmt.Println("period_aggregate:", period_aggregate)

	if paqp.StartTimestamp != start_timestamp {
		before_start_aggregate := getPeriodAggregate(start_timestamp, paqp.StartTimestamp)
		period_aggregate = mergeAggregates(period_aggregate, before_start_aggregate)
	}

	if paqp.EndTimestamp + paqp.Seconds != end_timestamp {
		after_end_aggregate := getPeriodAggregate(paqp.EndTimestamp + paqp.Seconds, end_timestamp)
		period_aggregate = mergeAggregates(period_aggregate, after_end_aggregate)
	}

	return period_aggregate
}

func mergeAggregates(agg1, agg2 PeriodAggregate) PeriodAggregate {
	var mergedAggregate PeriodAggregate

	if agg1.MinResponseTime < agg2.MinResponseTime {
		mergedAggregate.MinResponseTime = agg1.MinResponseTime
	} else {
		mergedAggregate.MinResponseTime = agg2.MinResponseTime
	}

	if agg1.MaxResponseTime > agg2.MaxResponseTime {
		mergedAggregate.MaxResponseTime = agg1.MaxResponseTime
	} else {
		mergedAggregate.MaxResponseTime = agg2.MaxResponseTime
	}
	
	mergedAggregate.TotalResponseTime = agg1.TotalResponseTime + agg2.TotalResponseTime
	mergedAggregate.TotalCount = agg1.TotalCount + agg2.TotalCount

	return mergedAggregate
}

func GetLongestPeriods(start_timestamp uint32, end_timestamp uint32) periodAggregateQueryParams {
	var paqp periodAggregateQueryParams

	min_threshold := uint32(60)

	diff := end_timestamp - start_timestamp

	if diff < min_threshold {
		paqp.StartTimestamp = 0
		paqp.EndTimestamp = 0
		paqp.Period = "NIL_PERIOD"

		return paqp
	}

	periods_in_seconds := periodsInSecondsReversed()

	for _, num_seconds := range(periods_in_seconds) {
		if num_seconds > diff {
			continue
		}

		var next_period_start uint32
		if start_timestamp % num_seconds == 0 {
			next_period_start = start_timestamp
		} else {
			next_period_start = start_timestamp - start_timestamp % num_seconds + num_seconds
		}

		if next_period_start + num_seconds <= end_timestamp {
			paqp.StartTimestamp = next_period_start
			paqp.Period = periodFromSeconds(num_seconds)
			paqp.Seconds = num_seconds

			delta := num_seconds
			for next_period_start + delta <= end_timestamp {
				paqp.EndTimestamp = next_period_start + delta - num_seconds

				delta += num_seconds
			}
			break
		}
	}

	return paqp
}

func nilPeriodAggregate() PeriodAggregate {
	var pa PeriodAggregate

	pa.MinResponseTime = 4294967296
	pa.MaxResponseTime = 0
	pa.TotalResponseTime = 0
	pa.TotalCount = 0

	return pa
}

func getPeriodAggregateForTimestampRange(paqp periodAggregateQueryParams) PeriodAggregate {
	var pa PeriodAggregate
	pa = nilPeriodAggregate()

    select_query := fmt.Sprintf(
    	"SELECT min_response_time, max_response_time, total_response_time, total_count FROM period_aggregates WHERE start_timestamp >= %d AND start_timestamp <= %d AND period = '%s'",
    	paqp.StartTimestamp,
    	paqp.EndTimestamp,
    	paqp.Period,
    )

    results, err := db.Query(select_query)

    if err != nil {
        panic(err.Error())
    }

    var paCurr PeriodAggregate
	for results.Next() {
		err = results.Scan(&paCurr.MinResponseTime, &paCurr.MaxResponseTime, &paCurr.TotalResponseTime, &paCurr.TotalCount)
		if err != nil {
			panic(err.Error())
		}
		pa = mergeAggregates(pa, paCurr)
	}

	return pa
}

// Given epoch timestamp and a period return the starting timestamp
// Working with GMT timezone for now
func getStartTimestampOfPeriod(timestamp uint32, period string) uint32 {
	seconds := secondsFromPeriod(period)
	return timestamp - timestamp % seconds
}

func getEndTimestampOfPeriod(timestamp uint32, period string) uint32 {
	start_timestamp := getStartTimestampOfPeriod(timestamp, period)
	if start_timestamp == timestamp {
		return start_timestamp
	} else {
		return start_timestamp + secondsFromPeriod(period)
	}
}

func periodFromSeconds(seconds uint32) string {
	var period string

	switch seconds {
	case 60:
		period = "ONE_MINUTE"
	case 180:
		period = "THREE_MINUTES"
	case 600:
		period = "TEN_MINUTES"
	case 1800:
		period = "THIRTY_MINUTES"
	case 3600:
		period = "ONE_HOUR"
	case 10800:
		period = "THREE_HOURS"
	case 21600:
		period = "SIX_HOURS"
	case 43200:
		period = "TWELVE_HOURS"
	case 86400:
		period = "ONE_DAY"
	default:
		panic(fmt.Sprintf("No periods found for seconds: %s", seconds))
	}

	return period
}

func secondsFromPeriod(period string) uint32 {
	var seconds uint32
	switch period {
	case "ONE_MINUTE":
		seconds = 60
	case "THREE_MINUTES":
		seconds = 180
	case "TEN_MINUTES":
		seconds = 600
	case "THIRTY_MINUTES":
		seconds = 1800
	case "ONE_HOUR":
		seconds = 3600
	case "THREE_HOURS":
		seconds = 10800
	case "SIX_HOURS":
		seconds = 21600
	case "TWELVE_HOURS":
		seconds = 43200
	case "ONE_DAY":
		seconds = 86400
	default:
		panic(fmt.Sprintf("Unknown period specified: %s", period))
	}

	return seconds
}

func periodsInSecondsReversed() []uint32 {
	return []uint32{86400, 43200, 21600, 10800, 3600, 1800, 600, 180, 60}
}