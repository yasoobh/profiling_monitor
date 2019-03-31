package main

import (
	"time"
	"math"
	"fmt"
	// "profiling_monitor/analyzer"
	"profiling_monitor/simulator"
	"profiling_monitor/ingestor"
)

func main() {
	day_throughput_amplitude := 10000.0
	night_throughput_amplitude := 14000.0

	morning_response_time_amplitude := 750.0
	day_response_time_amplitude := 1750.0
	night_response_time_amplitude := 900.0

	ds := simulator.DaySimulator{
		morning_response_time_amplitude,
		day_response_time_amplitude,
		night_response_time_amplitude,
		day_throughput_amplitude,
		night_throughput_amplitude,
	}

	t, _ := time.Parse(time.RFC3339, "2019-03-21T00:00:00Z")
	day_start_timestamp := t.Unix()

	var timestamp uint32
	// response_time_records := ds.GetMinuteResponseTimes(1263)

	response_time_records := ds.GetAllResponseTimes()
	for _, response_time_record := range(response_time_records) {
		fmt.Println(response_time_record)
		timestamp = uint32(day_start_timestamp) + uint32(math.Floor(response_time_record.Timestamp))*60
		fmt.Println(timestamp)
		ingestor.Ingest(timestamp, response_time_record.ResponseTime)
	}
	// ingestor.Release()
}