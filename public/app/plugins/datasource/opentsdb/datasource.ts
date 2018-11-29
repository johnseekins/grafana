import angular from 'angular';
import _ from 'lodash';
import { dateMath, DataQueryRequest, DataSourceApi } from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';
import { TemplateSrv } from 'app/features/templating/template_srv';
import { OpenTsdbOptions, OpenTsdbQuery } from './types';

export default class OpenTsDatasource extends DataSourceApi<OpenTsdbQuery, OpenTsdbOptions> {
  type: any;
  url: any;
  name: any;
  withCredentials: any;
  basicAuth: any;
  tsdbVersion: any;
  tsdbResolution: any;
  supportMetrics: any;
  lookupLimit: any;
  tagKeys: any;

  aggregatorsPromise: any;
  filterTypesPromise: any;

  /** @ngInject */
  constructor(instanceSettings: any, private templateSrv: TemplateSrv) {
    super(instanceSettings);
    this.type = 'opentsdb';
    this.url = instanceSettings.url;
    this.name = instanceSettings.name;
    this.withCredentials = instanceSettings.withCredentials;
    this.basicAuth = instanceSettings.basicAuth;
    instanceSettings.jsonData = instanceSettings.jsonData || {};
    this.tsdbVersion = instanceSettings.jsonData.tsdbVersion || 1;
    this.tsdbResolution = instanceSettings.jsonData.tsdbResolution || 1;
    this.lookupLimit = instanceSettings.jsonData.lookupLimit || 1000;
    this.supportMetrics = true;
    this.tagKeys = {};

    this.aggregatorsPromise = null;
    this.filterTypesPromise = null;
  }

  // Called once per panel (graph)
  query(options: DataQueryRequest<OpenTsdbQuery>) {
    const start = this.convertToTSDBTime(options.rangeRaw.from, false, options.timezone);
    const end = this.convertToTSDBTime(options.rangeRaw.to, true, options.timezone);
    const qs: any[] = [];
    const gExps: any[] = [];

    _.each(options.targets, target => {
      if (!target.metric && !target.gexp) {
        return;
      }
      if (!target.queryType) {
        target.queryType = 'metric';
      }
      if (target.queryType === 'metric') {
        qs.push(this.convertTargetToQuery(target, options, this.tsdbVersion));
      } else if (target.queryType === 'gexp') {
        gExps.push(this.convertGExpToQuery(target));
      }
    });

    const queries = _.compact(qs);
    const gExpressions = _.compact(gExps);

    // No valid targets, return the empty result to save a round trip.
    if (_.isEmpty(queries) && _.isEmpty(gExpressions)) {
      return Promise.resolve({ data: [] });
    }

    const groupByTags: any = {};
    _.each(queries, query => {
      if (query.filters && query.filters.length > 0) {
        _.each(query.filters, val => {
          groupByTags[val.tagk] = true;
        });
      } else {
        _.each(query.tags, (val, key) => {
          groupByTags[key] = true;
        });
      }
    });

    let queriesPromise;
    if (queries.length > 0) {
      queriesPromise = this.performTimeSeriesQuery(queries, start, end).then(response => {
        // only index into classic 'metrics' queries
        const tsqTargets = options.targets.filter(target => {
          return target.queryType === 'metric';
        });

        const metricToTargetMapping = this.mapMetricsToTargets(response.data, options, tsqTargets, this.tsdbVersion);

        const result = _.map(response.data, (metricData: any, index: number) => {
          index = metricToTargetMapping[index];
          if (index === -1) {
            index = 0;
          }
          this._saveTagKeys(metricData);

          return this.transformMetricData(metricData, groupByTags, tsqTargets[index], options, this.tsdbResolution);
        });
        return result;
      });
    }

    // perform single gExp queries so that we can reliably map targets to results once all the promises are resolved
    // (/query/gexp can perform combined queries but the result order is not determinate)
    const gexpPromises: Array<Promise<any>> = [];
    if (gExpressions.length > 0) {
      for (let gexpIndex = 0; gexpIndex < gExpressions.length; gexpIndex++) {
        const gexpPromise = this.performGExpressionQuery(gexpIndex, gExpressions[gexpIndex], start, end, options).then(
          (response: any) => {
            // only index into gexp queries
            const gexpTargets = options.targets.filter(target => {
              return target.queryType === 'gexp';
            });

            const gExpTargetIndex = this.mapGExpToTargets(response.config.url);

            const result = _.map(response.data, gexpData => {
              let index = gExpTargetIndex;
              if (index === -1) {
                index = 0;
              }
              return this.transformGexpData(gexpData, gexpTargets[index], this.tsdbResolution);
            });

            return result.filter(value => {
              return value !== false;
            });
          }
        );

        gexpPromises.push(gexpPromise);
      }
    }

    // call all queries into an array and concaternate their data into a return object
    const tsdbQueryPromises = [queriesPromise].concat(gexpPromises);

    // q.all([]) resolves all promises while keeping order in the return array
    // (see: https://docs.angularjs.org/api/ng/service/$q#all)
    return Promise.all(tsdbQueryPromises).then(responses => {
      let data: any = [];

      // response 0 from queriesPromise
      const queriesData = responses[0];
      if (queriesData && queriesData.length > 0) {
        data = data.concat(queriesData);
      }

      // response 1+ from gexpPromises
      for (const gexpData of responses.slice(1)) {
        data = data.concat(gexpData);
      }

      return {
        data: data,
      };
    });
  }

  annotationQuery(options: any) {
    const start = this.convertToTSDBTime(options.rangeRaw.from, false, options.timezone);
    const end = this.convertToTSDBTime(options.rangeRaw.to, true, options.timezone);
    const qs = [];
    const eventList: any[] = [];

    qs.push({ aggregator: 'sum', metric: options.annotation.target });

    const queries = _.compact(qs);

    return this.performTimeSeriesQuery(queries, start, end).then((results: any) => {
      if (results.data[0]) {
        let annotationObject = results.data[0].annotations;
        if (options.annotation.isGlobal) {
          annotationObject = results.data[0].globalAnnotations;
        }
        if (annotationObject) {
          _.each(annotationObject, annotation => {
            const event = {
              text: annotation.description,
              time: Math.floor(annotation.startTime) * 1000,
              annotation: options.annotation,
            };

            eventList.push(event);
          });
        }
      }
      return eventList;
    });
  }

  targetContainsTemplate(target: any) {
    if (target.filters && target.filters.length > 0) {
      for (let i = 0; i < target.filters.length; i++) {
        if (this.templateSrv.variableExists(target.filters[i].filter)) {
          return true;
        }
      }
    }

    if (target.tags && Object.keys(target.tags).length > 0) {
      for (const tagKey in target.tags) {
        if (this.templateSrv.variableExists(target.tags[tagKey])) {
          return true;
        }
      }
    }

    return false;
  }

  performTimeSeriesQuery(queries: any[], start: any, end: any) {
    let msResolution = false;
    if (this.tsdbResolution === 2) {
      msResolution = true;
    }
    const reqBody: any = {
      start: start,
      queries: queries,
      msResolution: msResolution,
      globalAnnotations: true,
    };
    if (this.tsdbVersion === 3) {
      reqBody.showQuery = true;
    }

    // Relative queries (e.g. last hour) don't include an end time
    if (end) {
      reqBody.end = end;
    }

    const options = {
      method: 'POST',
      url: this.url + '/api/query',
      data: reqBody,
    };

    this._addCredentialOptions(options);
    return getBackendSrv().datasourceRequest(options);
  }

  // retrieve a single gExp via GET to /api/query/gexp
  performGExpressionQuery(idx: number, gExp: string, start: any, end: any, globalOptions: any) {
    let urlParams = '?start=' + start + '&exp=' + gExp + '&gexpIndex=' + idx;
    urlParams = this.templateSrv.replace(urlParams, globalOptions.scopedVars, 'pipe');
    const options = {
      method: 'GET',
      url: this.url + '/api/query/gexp' + urlParams,
    };

    // Relative queries (e.g. last hour) don't include an end time
    if (end) {
      urlParams = '&end=' + end;
    }

    this._addCredentialOptions(options);
    return getBackendSrv().datasourceRequest(options);
  }

  suggestTagKeys(metric: string | number) {
    return Promise.resolve(this.tagKeys[metric] || []);
  }

  _saveTagKeys(metricData: { tags: {}; aggregateTags: any; metric: string | number }) {
    const tagKeys = Object.keys(metricData.tags);
    _.each(metricData.aggregateTags, tag => {
      tagKeys.push(tag);
    });

    this.tagKeys[metricData.metric] = tagKeys;
  }

  _performSuggestQuery(query: string, type: string) {
    return this._get('/api/suggest', { type, q: query, max: this.lookupLimit }).then((result: any) => {
      return result.data;
    });
  }

  _performMetricKeyValueLookup(metric: string, keys: any) {
    if (!metric || !keys) {
      return Promise.resolve([]);
    }

    const keysArray = keys.split(',').map((key: any) => {
      return key.trim();
    });
    const key = keysArray[0];
    let keysQuery = key + '=*';

    if (keysArray.length > 1) {
      keysQuery += ',' + keysArray.splice(1).join(',');
    }

    const m = metric + '{' + keysQuery + '}';

    return this._get('/api/search/lookup', { m: m, limit: this.lookupLimit }).then((result: any) => {
      result = result.data.results;
      const tagvs: any[] = [];
      _.each(result, r => {
        if (tagvs.indexOf(r.tags[key]) === -1) {
          tagvs.push(r.tags[key]);
        }
      });
      return tagvs;
    });
  }

  _performMetricKeyLookup(metric: any) {
    if (!metric) {
      return Promise.resolve([]);
    }

    return this._get('/api/search/lookup', { m: metric, limit: 1000 }).then((result: any) => {
      result = result.data.results;
      const tagks: any[] = [];
      _.each(result, r => {
        _.each(r.tags, (tagv, tagk) => {
          if (tagks.indexOf(tagk) === -1) {
            tagks.push(tagk);
          }
        });
      });
      return tagks;
    });
  }

  _get(relativeUrl: string, params?: { type?: string; q?: string; max?: number; m?: any; limit?: number }) {
    const options = {
      method: 'GET',
      url: this.url + relativeUrl,
      params: params,
    };

    this._addCredentialOptions(options);

    return getBackendSrv().datasourceRequest(options);
  }

  _addCredentialOptions(options: any) {
    if (this.basicAuth || this.withCredentials) {
      options.withCredentials = true;
    }
    if (this.basicAuth) {
      options.headers = { Authorization: this.basicAuth };
    }
  }

  metricFindQuery(query: string) {
    if (!query) {
      return Promise.resolve([]);
    }

    let interpolated;
    try {
      interpolated = this.templateSrv.replace(query, {}, 'distributed');
    } catch (err) {
      return Promise.reject(err);
    }

    const responseTransform = (result: any) => {
      return _.map(result, value => {
        return { text: value };
      });
    };

    const metricsRegex = /metrics\((.*)\)/;
    const tagNamesRegex = /tag_names\((.*)\)/;
    const tagValuesRegex = /tag_values\((.*?),\s?(.*)\)/;
    const tagNamesSuggestRegex = /suggest_tagk\((.*)\)/;
    const tagValuesSuggestRegex = /suggest_tagv\((.*)\)/;

    const metricsQuery = interpolated.match(metricsRegex);
    if (metricsQuery) {
      return this._performSuggestQuery(metricsQuery[1], 'metrics').then(responseTransform);
    }

    const tagNamesQuery = interpolated.match(tagNamesRegex);
    if (tagNamesQuery) {
      return this._performMetricKeyLookup(tagNamesQuery[1]).then(responseTransform);
    }

    const tagValuesQuery = interpolated.match(tagValuesRegex);
    if (tagValuesQuery) {
      return this._performMetricKeyValueLookup(tagValuesQuery[1], tagValuesQuery[2]).then(responseTransform);
    }

    const tagNamesSuggestQuery = interpolated.match(tagNamesSuggestRegex);
    if (tagNamesSuggestQuery) {
      return this._performSuggestQuery(tagNamesSuggestQuery[1], 'tagk').then(responseTransform);
    }

    const tagValuesSuggestQuery = interpolated.match(tagValuesSuggestRegex);
    if (tagValuesSuggestQuery) {
      return this._performSuggestQuery(tagValuesSuggestQuery[1], 'tagv').then(responseTransform);
    }

    return Promise.resolve([]);
  }

  testDatasource() {
    return this._performSuggestQuery('cpu', 'metrics').then(() => {
      return { status: 'success', message: 'Data source is working' };
    });
  }

  getAggregators() {
    if (this.aggregatorsPromise) {
      return this.aggregatorsPromise;
    }

    this.aggregatorsPromise = this._get('/api/aggregators').then((result: any) => {
      if (result.data && _.isArray(result.data)) {
        return result.data.sort();
      }
      return [];
    });
    return this.aggregatorsPromise;
  }

  getFilterTypes() {
    if (this.filterTypesPromise) {
      return this.filterTypesPromise;
    }

    this.filterTypesPromise = this._get('/api/config/filters').then((result: any) => {
      if (result.data) {
        return Object.keys(result.data).sort();
      }
      return [];
    });
    return this.filterTypesPromise;
  }

  transformMetricData(md: { dps: any }, groupByTags: any, target: any, options: any, tsdbResolution: number) {
    const metricLabel = this.createMetricLabel(md, target, groupByTags, options);
    const dps = this.getDatapointsAtCorrectResolution(md, tsdbResolution);

    return { target: metricLabel, datapoints: dps };
  }

  transformGexpData(gExp: string, target: any, tsdbResolution: number) {
    if (typeof target === 'undefined') {
      // the metric is hidden
      return false;
    }

    const metricLabel = this.createGexpLabel(gExp, target);
    const dps = this.getDatapointsAtCorrectResolution(gExp, tsdbResolution);

    return { target: metricLabel, datapoints: dps };
  }

  getDatapointsAtCorrectResolution(result: any, tsdbResolution: number) {
    const dps: any[] = [];

    // TSDB returns datapoints has a hash of ts => value.
    // Can't use _.pairs(invert()) because it stringifies keys/values
    _.each(result.dps, (v: any, k: any) => {
      if (tsdbResolution === 2) {
        dps.push([v, k * 1]);
      } else {
        dps.push([v, k * 1000]);
      }
    });

    return dps;
  }

  createMetricLabel(
    md: { dps?: any; tags?: any; metric?: any },
    target: { alias: string },
    groupByTags: any,
    options: { scopedVars: any }
  ) {
    if (target.alias) {
      const scopedVars = _.clone(options.scopedVars || {});
      _.each(md.tags, (value, key) => {
        scopedVars['tag_' + key] = { value: value };
      });
      return this.templateSrv.replace(target.alias, scopedVars);
    }

    let label = md.metric;
    const tagData: any[] = [];

    if (!_.isEmpty(md.tags)) {
      _.each(_.toPairs(md.tags), tag => {
        if (_.has(groupByTags, tag[0])) {
          tagData.push(tag[0] + '=' + tag[1]);
        }
      });
    }

    if (!_.isEmpty(tagData)) {
      label += '{' + tagData.join(', ') + '}';
    }

    return label;
  }

  createGexpLabel(data: any, target: any) {
    if (!target.gexpAlias) {
      return target.gexp;
    }

    return target.gexpAlias.replace(/\$tag_([a-zA-Z0-9-_\.\/]+)/g, (all: string, m1: string) => data.tags[m1]);
  }

  convertGExpToQuery(target: any) {
    // filter out a target if it is 'hidden'
    if (target.hide === true) {
      return null;
    }

    return target.gexp;
  }

  convertTargetToQuery(target: any, options: any, tsdbVersion: number) {
    if (!target.metric || target.hide) {
      return null;
    }

    const query: any = {
      metric: this.templateSrv.replace(target.metric, options.scopedVars, 'pipe'),
      aggregator: 'avg',
    };

    if (target.aggregator) {
      query.aggregator = this.templateSrv.replace(target.aggregator);
    }

    if (target.shouldComputeRate) {
      query.rate = true;
      query.rateOptions = {
        counter: !!target.isCounter,
      };

      if (target.counterMax && target.counterMax.length) {
        query.rateOptions.counterMax = parseInt(target.counterMax, 10);
      }

      if (target.counterResetValue && target.counterResetValue.length) {
        query.rateOptions.resetValue = parseInt(target.counterResetValue, 10);
      }

      if (tsdbVersion >= 2) {
        query.rateOptions.dropResets =
          !query.rateOptions.counterMax && (!query.rateOptions.ResetValue || query.rateOptions.ResetValue === 0);
      }
    }

    if (!target.disableDownsampling) {
      let interval = this.templateSrv.replace(target.downsampleInterval || options.interval);

      if (interval.match(/\.[0-9]+s/)) {
        interval = parseFloat(interval) * 1000 + 'ms';
      }

      query.downsample = interval + '-' + target.downsampleAggregator;

      if (target.downsampleFillPolicy && target.downsampleFillPolicy !== 'none') {
        query.downsample += '-' + target.downsampleFillPolicy;
      }
    }

    if (target.filters && target.filters.length > 0) {
      query.filters = angular.copy(target.filters);
      if (query.filters) {
        for (const filterKey in query.filters) {
          query.filters[filterKey].filter = this.templateSrv.replace(
            query.filters[filterKey].filter,
            options.scopedVars,
            'pipe'
          );
        }
      }
    } else {
      query.tags = angular.copy(target.tags);
      if (query.tags) {
        for (const tagKey in query.tags) {
          query.tags[tagKey] = this.templateSrv.replace(query.tags[tagKey], options.scopedVars, 'pipe');
        }
      }
    }

    if (target.explicitTags) {
      query.explicitTags = true;
    }

    return query;
  }

  mapMetricsToTargets(metrics: any, targets: any, options: any, tsdbVersion: number) {
    let interpolatedTagValue, arrTagV;
    return _.map(metrics, metricData => {
      if (tsdbVersion === 3) {
        return metricData.query.index;
      } else {
        return _.findIndex(options.targets as any[], target => {
          if (target.filters && target.filters.length > 0) {
            return target.metric === metricData.metric;
          } else {
            return (
              target.metric === metricData.metric &&
              _.every(target.tags, (tagV, tagK) => {
                interpolatedTagValue = this.templateSrv.replace(tagV, options.scopedVars, 'pipe');
                arrTagV = interpolatedTagValue.split('|');
                return _.includes(arrTagV, metricData.tags[tagK]) || interpolatedTagValue === '*';
              })
            );
          }
        });
      }
    });
  }

  mapGExpToTargets(queryUrl: string): number {
    // extract gexpIndex from URL
    const regex = /.+gexpIndex=(\d+).*/;
    const gexpIndex = queryUrl.match(regex);

    if (!gexpIndex) {
      return -1;
    }

    return Number(gexpIndex[1]);
  }

  convertToTSDBTime(date: any, roundUp: any, timezone: any) {
    if (date === 'now') {
      return null;
    }

    date = dateMath.parse(date, roundUp, timezone);
    return date.valueOf();
  }
}
