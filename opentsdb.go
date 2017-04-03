package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/net/context/ctxhttp"
)

type queryable struct{}

func (q *queryable) Querier(mint int64, maxt int64) (storage.Querier, error) {
	return &querier{
		client: &OpenTSDBClient{
			URL:        config.openTSDBurl,
			httpClient: http.DefaultClient,
		},
		mint: mint,
		maxt: maxt}, nil
}

func (q *queryable) Appender() (storage.Appender, error) {
	panic("not implemented")
}

func (s *queryable) Close() error {
	panic("not implemented")
}

type querier struct {
	client        *OpenTSDBClient
	labelMatchers []*labels.Matcher
	mint, maxt    int64
}

func (q *querier) Select(m ...*labels.Matcher) storage.SeriesSet {
	// FIXME Set context
	// FIXME Check for compatible API version: http://opentsdb.net/docs/build/html/api_http/version.html
	q.labelMatchers = m
	data, err := q.client.Query(context.TODO(), *q)
	if err != nil {
		return &seriesSet{
			err: err,
		}
	}

	return &seriesSet{series: data, cursor: -1}
}

func (q *querier) LabelValues(name string) ([]string, error) {
	if name == labels.MetricName {
		// FIXME
		return []string{"not yet implemented"}, nil
	}

	// FIXME Implement
	return []string{}, errors.New("Only /api/v1/label/__name__/values is supported")
}

func (q *querier) Close() error {
	// Do nothing
	return nil
}

type seriesSet struct {
	series []OpenTSDBResponse
	cursor int
	err    error
}

func (ss *seriesSet) Next() bool {
	if ss.cursor == len(ss.series)-1 {
		return false
	}

	ss.cursor++
	return true
}

func (ss *seriesSet) At() storage.Series {
	return &ss.series[ss.cursor]
}

func (ss *seriesSet) Err() error {
	return ss.err
}

type sample struct {
	timestamp int64
	value     float64
}

type OpenTSDBResponse struct {
	Tags       tags       `json:"tags"`
	Metric     string     `json:"metric"`
	Datapoints datapoints `json:"dps"`
}

type datapoints map[string]float64

func (d datapoints) samples() []sample {
	ret := make([]sample, 0, len(d))
	for ts, v := range d {
		time, _ := strconv.ParseInt(ts, 10, 64)
		time *= 1000
		ret = append(ret, sample{time, v})
	}

	sort.Slice(ret, func(i, j int) bool { return ret[i].timestamp < ret[j].timestamp })
	return ret
}

type tags map[string]string

func (t tags) labels() labels.Labels {
	ret := make(labels.Labels, 0, len(t))
	for n, v := range t {
		ret = append(ret, labels.Label{n, v})
	}

	sort.Slice(ret, func(i, j int) bool { return ret[i].Name < ret[j].Name })
	return ret
}

func (s *OpenTSDBResponse) Labels() labels.Labels {
	return append(s.Tags.labels(), labels.Label{
		labels.MetricName,
		s.Metric,
	})
}

func (s *OpenTSDBResponse) Iterator() storage.SeriesIterator {
	return &seriesIterator{
		samples: s.Datapoints.samples(),
		cursor:  -1,
	}
}

type seriesIterator struct {
	samples []sample
	cursor  int
	err     error
}

func (it *seriesIterator) Seek(t int64) bool {
	x := sort.Search(len(it.samples), func(i int) bool { return it.samples[i].timestamp >= t })

	// Index not found, use the last one (which will be the closest)
	if x == len(it.samples) {
		x--
	}

	it.cursor = x
	return true
}

func (it *seriesIterator) At() (t int64, v float64) {
	s := it.samples[it.cursor]
	return s.timestamp, s.value
}

func (it *seriesIterator) Next() bool {
	if it.cursor == len(it.samples)-1 {
		return false
	}

	it.cursor++
	return true
}

func (it *seriesIterator) Err() error {
	return it.err
}

type OpenTSDBClient struct {
	BasicAuth         bool
	BasicAuthUser     string
	BasicAuthPassword string
	URL               string

	httpClient *http.Client
}

func (c *OpenTSDBClient) Query(ctx context.Context, query querier) ([]OpenTSDBResponse, error) {
	r := &OpenTSDBRequest{query.mint, query.maxt, nil}

	metric := OpenTSDBQuery{
		Aggregator:   "none",
		MsResolution: true,
	}

	for _, m := range query.labelMatchers {
		if m.Name == labels.MetricName {
			if m.Type == labels.MatchEqual {
				metric.Metric = m.Value
				continue
			}

			return []OpenTSDBResponse{}, errors.New("Can't support not or regex for metric names")
		}

		f := &Filter{
			// FIXME maybe we can optimise this
			Type:   "regexp",
			TagK:   m.Name,
			Filter: m.Value,
		}
		metric.Filters = append(metric.Filters, *f)
	}

	r.Queries = append(r.Queries, metric)

	req, err := c.createRequest(*r)
	if err != nil {
		return nil, err
	}

	res, err := ctxhttp.Do(ctx, c.httpClient, req)
	if err != nil {
		return nil, err
	}

	queryResult, err := c.parseResponse(*r, res)
	if err != nil {
		return nil, err
	}

	return queryResult, nil
}

func (c *OpenTSDBClient) createRequest(data OpenTSDBRequest) (*http.Request, error) {
	u, _ := url.Parse(c.URL)
	u.Path = path.Join(u.Path, "api/query")

	postData, err := json.Marshal(data)

	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(string(postData)))
	if err != nil {
		return nil, fmt.Errorf("Failed to create request. error: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.BasicAuth {
		req.SetBasicAuth(c.BasicAuthUser, c.BasicAuthPassword)
	}

	return req, err
}

func (e *OpenTSDBClient) parseResponse(query OpenTSDBRequest, res *http.Response) ([]OpenTSDBResponse, error) {
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}

	if res.StatusCode/100 != 2 {
		s := string(body)
		// Return empty result set when metric does not exit
		// FIXME: May want to return an error and check for it upstream
		if strings.Contains(s, "No such name for 'metrics'") {
			return []OpenTSDBResponse{}, nil
		}

		return nil, fmt.Errorf("Request failed status: %v", res.Status)
	}

	var data []OpenTSDBResponse
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	// Filter out empty rows, Prometheus expects no data when there are no datapoints
	ret := data[:0]
	for _, d := range data {
		if len(d.Datapoints) > 0 {
			ret = append(ret, d)
		}
	}

	return ret, nil
}

/*func (e *OpenTsdbExecutor) buildMetric(query *tsdb.Query) map[string]interface{} {

metric := make(map[string]interface{})
// Setting metric and aggregator
metric["metric"] = query.Model.Get("metric").MustString()
//metric["aggregator"] = query.Model.Get("aggregator").MustString()

// Setting downsampling options
/*disableDownsampling := query.Model.Get("disableDownsampling").MustBool()
if !disableDownsampling {
	downsampleInterval := query.Model.Get("downsampleInterval").MustString()
	if downsampleInterval == "" {
		downsampleInterval = "1m" //default value for blank
	}
	downsample := downsampleInterval + "-" + query.Model.Get("downsampleAggregator").MustString()
	if query.Model.Get("downsampleFillPolicy").MustString() != "none" {
		metric["downsample"] = downsample + "-" + query.Model.Get("downsampleFillPolicy").MustString()
	} else {
		metric["downsample"] = downsample
	}
}*/

// Setting rate options
/*if query.Model.Get("shouldComputeRate").MustBool() {

	metric["rate"] = true
	rateOptions := make(map[string]interface{})
	rateOptions["counter"] = query.Model.Get("isCounter").MustBool()

	counterMax, counterMaxCheck := query.Model.CheckGet("counterMax")
	if counterMaxCheck {
		rateOptions["counterMax"] = counterMax.MustFloat64()
	}

	resetValue, resetValueCheck := query.Model.CheckGet("counterResetValue")
	if resetValueCheck {
		rateOptions["resetValue"] = resetValue.MustFloat64()
	}

	if !counterMaxCheck && (!resetValueCheck || resetValue.MustFloat64() == 0) {
		rateOptions["dropcounter"] = true
	}

	metric["rateOptions"] = rateOptions
}*/

// Setting tags
/*	tags, tagsCheck := query.Model.CheckGet("tags")
	if tagsCheck && len(tags.MustMap()) > 0 {
		metric["tags"] = tags.MustMap()
	}

	// Setting filters
	filters, filtersCheck := query.Model.CheckGet("filters")
	if filtersCheck && len(filters.MustArray()) > 0 {
		metric["filters"] = filters.MustArray()
	}

	return metric

}*/

type OpenTSDBQuery struct {
	Aggregator   string   `json:"aggregator"`
	Metric       string   `json:"metric"`
	MsResolution bool     `json:"msResolution"`
	Filters      []Filter `json:"filters,omitempty"`
}

type OpenTSDBRequest struct {
	Start   int64           `json:"start"`
	End     int64           `json:"end"`
	Queries []OpenTSDBQuery `json:"queries"`
}

type Filter struct {
	Type    string `json:"type"`
	TagK    string `json:"tagk"`
	Filter  string `json:"filter"`
	GroupBy bool   `json:"groupBy"`
}
