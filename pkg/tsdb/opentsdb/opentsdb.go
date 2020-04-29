package opentsdb

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"golang.org/x/net/context/ctxhttp"

	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/grafana/grafana/pkg/components/null"
	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/models"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/tsdb"
)

type OpenTsdbExecutor struct {
}

func NewOpenTsdbExecutor(datasource *models.DataSource) (tsdb.TsdbQueryEndpoint, error) {
	return &OpenTsdbExecutor{}, nil
}

var (
	plog log.Logger
)

func init() {
	plog = log.New("tsdb.opentsdb")
	tsdb.RegisterTsdbQueryEndpoint("opentsdb", NewOpenTsdbExecutor)
}

func (e *OpenTsdbExecutor) Query(ctx context.Context, dsInfo *models.DataSource, queryContext *tsdb.TsdbQuery) (*tsdb.Response, error) {
	queryResult := tsdb.NewQueryResult()

	start := queryContext.TimeRange.GetFromAsMsEpoch()
	end := queryContext.TimeRange.GetToAsMsEpoch()

	metricQueries := make([]map[string]interface{}, 0)
	gexpQueries := make([]string, 0)

	for _, query := range queryContext.Queries {
		queryTypeJson, hasQueryType := query.Model.CheckGet("queryType")
		queryType := queryTypeJson.MustString()
		if !hasQueryType || queryType == "metric" {
			metricQueries = append(metricQueries, e.buildMetric(query))
		} else if queryType == "gexp" {
			gexpQueries = append(gexpQueries, e.buildGexp(query, start, end))
		} else {
			return nil, fmt.Errorf("Unrecognized query type: %v", queryType)
		}
	}

	httpClient, err := dsInfo.GetHttpClient()
	if err != nil {
		return nil, err
	}

	err = e.metricsRequest(dsInfo, ctx, httpClient, start, end, metricQueries, queryResult)
	if err != nil {
		return nil, err
	}

	for _, query := range gexpQueries {
		err := e.gexpRequest(dsInfo, ctx, httpClient, query, queryResult)
		if err != nil {
			return nil, err
		}
	}

	result := &tsdb.Response{}
	series := make(map[string]*tsdb.QueryResult)
	series["A"] = queryResult
	result.Results = series
	return result, nil
}

func (e *OpenTsdbExecutor) metricsRequest(dsInfo *models.DataSource, ctx context.Context, httpClient *http.Client, start int64, end int64, queries []map[string]interface{}, results *tsdb.QueryResult) error {
	if len(queries) == 0 {
		return nil
	}

	u, _ := url.Parse(dsInfo.Url)
	u.Path = path.Join(u.Path, "api/query")

	var metricsTsdbQuery = OpenTsdbMetricQuery{
		Start:   start,
		End:     end,
		Queries: queries,
	}

	if setting.Env == setting.DEV {
		plog.Debug("OpenTsdb metrics request", "params", metricsTsdbQuery)
	}
	plog.Info("OpenTsdb metrics request", "params", metricsTsdbQuery) // DEBUG

	postData, err := json.Marshal(metricsTsdbQuery)
	if err != nil {
		plog.Info("Failed marshaling data", "error", err)
		return fmt.Errorf("Failed to create request. error: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(string(postData)))
	if err != nil {
		plog.Info("Failed to create request", "error", err)
		return fmt.Errorf("Failed to create request. error: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if dsInfo.BasicAuth {
		req.SetBasicAuth(dsInfo.BasicAuthUser, dsInfo.DecryptedBasicAuthPassword())
	}

	res, err := ctxhttp.Do(ctx, httpClient, req)
	if err != nil {
		return err
	}

	err = e.parseResponse(res, results)
	if err != nil {
		return err
	}

	return nil
}

func (e *OpenTsdbExecutor) gexpRequest(dsInfo *models.DataSource, ctx context.Context, httpClient *http.Client, query string, results *tsdb.QueryResult) error {
	u, _ := url.Parse(dsInfo.Url)
	u.Path = path.Join(u.Path, "api/query/gexp")
	u.RawQuery = query

	if setting.Env == setting.DEV {
		plog.Debug("OpenTsdb gexp request", "query", query)
	}
	plog.Info("OpenTsdb gexp request", "query", query) // DEBUG

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("Failed to create request. error: %v", err)
	}

	if dsInfo.BasicAuth {
		req.SetBasicAuth(dsInfo.BasicAuthUser, dsInfo.DecryptedBasicAuthPassword())
	}

	res, err := ctxhttp.Do(ctx, httpClient, req)
	if err != nil {
		return err
	}

	err = e.parseResponse(res, results)
	if err != nil {
		return err
	}

	return nil
}

func (e *OpenTsdbExecutor) parseResponse(res *http.Response, results *tsdb.QueryResult) error {

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return err
	}

	if res.StatusCode/100 != 2 {
		plog.Info("Request failed", "status", res.Status, "body", string(body))
		return fmt.Errorf("Request failed status: %v", res.Status)
	}

	var data []OpenTsdbResponse
	err = json.Unmarshal(body, &data)
	if err != nil {
		plog.Info("Failed to unmarshal opentsdb response", "error", err, "status", res.Status, "body", string(body))
		return err
	}

	for _, val := range data {
		series := tsdb.TimeSeries{
			Name: val.Metric,
		}

		for timeString, value := range val.DataPoints {
			timestamp, err := strconv.ParseFloat(timeString, 64)
			if err != nil {
				plog.Info("Failed to unmarshal opentsdb timestamp", "timestamp", timeString)
				return err
			}
			series.Points = append(series.Points, tsdb.NewTimePoint(null.FloatFrom(value), timestamp))
		}

		results.Series = append(results.Series, &series)
	}

	return nil
}

func (e *OpenTsdbExecutor) buildMetric(query *tsdb.Query) map[string]interface{} {

	metric := make(map[string]interface{})

	// Setting metric and aggregator
	metric["metric"] = query.Model.Get("metric").MustString()
	metric["aggregator"] = query.Model.Get("aggregator").MustString()

	// Setting downsampling options
	disableDownsampling := query.Model.Get("disableDownsampling").MustBool()
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
	}

	// Setting rate options
	if query.Model.Get("shouldComputeRate").MustBool() {

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
			rateOptions["dropResets"] = true
		}

		metric["rateOptions"] = rateOptions
	}

	// Setting tags
	tags, tagsCheck := query.Model.CheckGet("tags")
	if tagsCheck && len(tags.MustMap()) > 0 {
		metric["tags"] = tags.MustMap()
	}

	// Setting filters
	filters, filtersCheck := query.Model.CheckGet("filters")
	if filtersCheck && len(filters.MustArray()) > 0 {
		metric["filters"] = filters.MustArray()
	}

	return metric

}

func (e *OpenTsdbExecutor) buildGexp(query *tsdb.Query, start int64, end int64) string {

	queryString := fmt.Sprintf("start=%d&end=%d", start, end)

	// Setting downsampling options
	disableDownsampling := query.Model.Get("disableDownsampling").MustBool()
	if !disableDownsampling {
		downsampleInterval := query.Model.Get("downsampleInterval").MustString()
		if downsampleInterval == "" {
			downsampleInterval = "1m" //default value for blank
		}
		downsample := downsampleInterval + "-" + query.Model.Get("downsampleAggregator").MustString()
		if query.Model.Get("downsampleFillPolicy").MustString() != "none" {
			queryString += "&downsampleFillPolicy=" + url.QueryEscape(downsample) + "-" + url.QueryEscape(query.Model.Get("downsampleFillPolicy").MustString())
		} else {
			queryString += "&downsampleFillPolicy=" + url.QueryEscape(downsample)
		}
	}

	queryString += "&exp=" + url.QueryEscape(query.Model.Get("gexp").MustString())

	return queryString
}
