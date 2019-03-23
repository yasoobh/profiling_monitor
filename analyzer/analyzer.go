package analyzer

import ("fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
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

func Analyze(start_timestamp, end_timestamp uint32) {
	fmt.Println("analyzing!")
	getPeriodAggregate(start_timestamp, end_timestamp)
}

func getPeriodAggregate(start_timestamp, end_timestamp uint32) {
	// diff = end - start
	// find biggest period which is contained in the range
	// go result for those big periods
	// go getPeriodAggregate() for the sub ranges

	paqp := GetLongestPeriods(start_timestamp, end_timestamp)
	fmt.Println(paqp.StartTimestamp)
	fmt.Println(paqp.EndTimestamp)
	period_aggregates := getPeriodAggregateForTimestampRange(paqp)
	
	// if paqp.StartTimestamp != start_timestamp {
	// 	before_start_aggregate := getPeriodAggregate(start_timestamp, paqp.StartTimestamp)
	// 	period_aggregate = mergeAggregates(period_aggregate, before_start_aggregate)
	// }

	// if paqp.EndTimestamp != end_timestamp {
	// 	after_end_aggregate := getPeriodAggregate(paqp.EndTimestamp, end_timestamp)
	// 	period_aggregate = mergeAggregates(period_aggregate, after_end_aggregate)
	// }

	// return period_aggregate
}

func mergeAggregates(agg1, agg2 map[string]float64) {

}

type periodAggregateQueryParams struct {
	StartTimestamp uint32
	EndTimestamp uint32
	Period string
}

func GetLongestPeriods(start_timestamp uint32, end_timestamp uint32) periodAggregateQueryParams {
	diff := end_timestamp - start_timestamp
	fmt.Println(diff)
	periods_in_seconds := periodsInSecondsReversed()

	var paqp periodAggregateQueryParams

	for _, num_seconds := range(periods_in_seconds) {
		fmt.Println(num_seconds)
		if num_seconds > diff {
			continue
		}

		next_period_start := start_timestamp - start_timestamp % num_seconds + num_seconds

		if next_period_start + num_seconds <= end_timestamp {
			paqp.StartTimestamp = next_period_start
			paqp.Period = periodFromSeconds(num_seconds)

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

type PeriodAggregate struct {
	StartTimestamp uint32
	Period string
	MinResponseTime float64
	MaxResponseTime float64
	TotalResponseTime float64
	TotalCount uint32
}

func getPeriodAggregateForTimestampRange(paqp periodAggregateQueryParams) []PeriodAggregate {
	var period_aggregates []PeriodAggregate

    select_query := fmt.Sprintf(
    	"SELECT start_timestamp, period, min_response_time, max_response_time, total_response_time, total_count FROM period_aggregates WHERE start_timestamp >= %d AND start_timestamp <= %d AND period = '%s'",
    	paqp.StartTimestamp,
    	paqp.EndTimestamp,
    	paqp.Period,
    )

    fmt.Println(select_query)

    results, err := db.Query(select_query)

    if err != nil {
        panic(err.Error())
    }

	for results.Next() {
		var pa PeriodAggregate

		err = results.Scan(&pa.StartTimestamp, &pa.Period, &pa.MinResponseTime, &pa.MaxResponseTime, &pa.TotalResponseTime, &pa.TotalCount)
		if err != nil {
			panic(err.Error())
		}
		fmt.Println(pa)

		period_aggregates = append(period_aggregates, pa)
	}

	return period_aggregates
}

// Given epoch timestamp and a period return the starting timestamp
// Working with GMT timezone for now
func getStartTimestampOfPeriod(timestamp uint32, period string) uint32 {
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

	return timestamp - timestamp % seconds
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

func periodsInSecondsReversed() []uint32 {
	return []uint32{86400, 43200, 21600, 10800, 3600, 1800, 600, 180, 60}
}