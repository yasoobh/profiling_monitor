package simulator

import ("fmt"
	"math/rand"
	"math"
	"strings"
)

type ResponseTimeRecord struct {
	Timestamp float64
	ResponseTime float64
}

func (rtr ResponseTimeRecord) String() string {
	return strings.Join([]string{"Timestamp: ", fmt.Sprintf("%f", rtr.Timestamp), ", Response Time: ", fmt.Sprintf("%f", rtr.ResponseTime), "\n"}, " ")
}

type DaySimulator struct {
	MorningResponseTimeAmplitude float64
	DayResponseTimeAmplitude float64
	NightResponseTimeAmplitude float64
	DayThroughputAmplitude float64
	NightThroughputAmplitude float64
}

func (ds DaySimulator) GetAllResponseTimes() []ResponseTimeRecord {
	var response_times []ResponseTimeRecord

	minute_wise_timestamps := ds.GetAllStartTimestamps()

	for _, minute_timestamp := range(minute_wise_timestamps) {
		response_times_minute := ds.GetMinuteResponseTimes(minute_timestamp)
		response_times = append(response_times, response_times_minute...)
	}

	return response_times
}

func (ds DaySimulator) GetMinuteResponseTimes(minute_timestamp uint16) []ResponseTimeRecord {
	var response_times_minute []ResponseTimeRecord

	rts := ResponseTimeSimulator{
		ds.MorningResponseTimeAmplitude,
		ds.DayResponseTimeAmplitude,
		ds.NightResponseTimeAmplitude,
	}

	ts := ThroughputSimulator{
		ds.DayThroughputAmplitude,
		ds.NightThroughputAmplitude,
	}

	curr_throughput := int(math.Floor(ts.GetThroughput(float64(minute_timestamp)/60.0)))

	rand.Seed(int64(minute_timestamp))

	var rtr ResponseTimeRecord

	for i := 0; i<curr_throughput; i++ {
		minute_timestamp_with_seconds := float64(minute_timestamp) + math.Floor(rand.Float64()*60.0)
		sample_response_time := rts.GetResponseTime(minute_timestamp_with_seconds/60.0)

		rtr = ResponseTimeRecord{
			minute_timestamp_with_seconds,
			sample_response_time,
		}

		response_times_minute = append(response_times_minute, rtr)
	}

	return response_times_minute
}

// 1 minute period; whole day
func (ds DaySimulator) GetAllStartTimestamps() []uint16 {
	var start_timestamps []uint16

	for i:=0; i<24; i++ {
		for j:=0; j<60; j++ {
			start_timestamps = append(start_timestamps, uint16(i*60+j))
		}
	}

	return start_timestamps
}