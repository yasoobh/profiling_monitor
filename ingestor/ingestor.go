package ingestor

import ("fmt"
	"github.com/go-redis/redis"
	"strings"
	"strconv"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var client *redis.Client

func init() {
	var err error
	db, err = sql.Open("mysql", "root:ongbak3@tcp(localhost)/profiling_monitor")
	if (err != nil) {
		panic(err.Error())
	}

    client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func Release() {
	db.Close()
}

func Ingest(timestamp uint32, response_time float64) {
	period_names := periods()

	for _, period := range(period_names) {
		period_aggregate := getPeriodAggregateFromRedis(period)
		start_timestamp := getStartTimestampOfPeriod(timestamp, period)

		period_aggregate_start_timestamp := period_aggregate["start_timestamp"]

		if uint32(period_aggregate_start_timestamp) == start_timestamp {
			if response_time < period_aggregate["min_response_time"] {
				period_aggregate["min_response_time"] = response_time
			}

			if response_time > period_aggregate["max_response_time"] {
				period_aggregate["max_response_time"] = response_time
			}

			period_aggregate["total_response_time"] += response_time
			period_aggregate["total_count"] += 1
			pushAggregateToRedis(period, period_aggregate)
		} else {
			// push current aggregate to mysql
			flushCurrentEntryToDb(period, period_aggregate)

			// store fresh aggregate in redis
			new_period_aggregate := map[string]float64 {
				"start_timestamp" : float64(start_timestamp),
				"min_response_time" : response_time,
				"max_response_time" : response_time,
				"total_response_time": response_time,
				"total_count" : 1,
			}
			pushAggregateToRedis(period, new_period_aggregate)
		}
	}
	defer fmt.Println("Ingested!")
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

func flushCurrentEntryToDb(period string, period_aggregate map[string]float64) {
    insert_query := fmt.Sprintf("INSERT INTO period_aggregates (period, start_timestamp, min_response_time, max_response_time, total_response_time, total_count) VALUES ('%s', %f, %f, %f, %f, %f)",
    	period,
    	period_aggregate["start_timestamp"],
    	period_aggregate["min_response_time"],
    	period_aggregate["max_response_time"],
    	period_aggregate["total_response_time"],
    	period_aggregate["total_count"],
    )

    fmt.Println(insert_query)

    insert, err := db.Query(insert_query)

    if err != nil {
        panic(err.Error())
    }

    defer insert.Close()
}

func pushAggregateToRedis(period string, period_aggregate map[string]float64) {
	redis_key := getRedisKeyFromPeriod(period)

	for metric_name, val := range(period_aggregate) {
		if metric_name == "start_timestamp" {
			val = float64(val)
		}

		bc := client.HSet(redis_key, metric_name, val)
		if bc.Err() != nil {
			fmt.Println(bc.Err())
		}
	}
}

func getPeriodAggregateFromRedis(period string) map[string]float64 {
	redis_key := getRedisKeyFromPeriod(period)

	period_aggregate := make(map[string]float64)

	metric_names := []string{"start_timestamp", "min_response_time", "max_response_time", "total_response_time", "total_count"}

	for _, metric_name := range(metric_names) {
		val, err := client.HGet(redis_key, metric_name).Result()
		if err != nil {
			// fmt.Println(redis_key, "not found")
		} else {
			val_float, _ := strconv.ParseFloat(val, 64)
			period_aggregate[metric_name] = val_float
		}
	}

	return period_aggregate
}

func getRedisKeyFromPeriod(period string) string {
	return strings.Join([]string{strings.ToLower(period), "_agg"}, "")
}

func periods() []string {
	return []string{"ONE_MINUTE","THREE_MINUTES","TEN_MINUTES","THIRTY_MINUTES","ONE_HOUR","THREE_HOURS","SIX_HOURS","TWELVE_HOURS","ONE_DAY"}
}
