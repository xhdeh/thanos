// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/benchutil"
)

func TestStoreSeriesSet(t *testing.T) {
	tb := testutil.NewTB(t)
	tb.Run(benchutil.OneSampleSeriesSubTestName(200e3), func(tb testutil.TB) {
		benchStoreSeriesSet(tb, 200e3, benchutil.SeriesDimension)
	})
	tb.Run(benchutil.OneSeriesManySamplesSubTestName(200e3), func(tb testutil.TB) {
		benchStoreSeriesSet(tb, 200e3, benchutil.SamplesDimension)
	})
}

func BenchmarkStoreSeriesSet(b *testing.B) {
	tb := testutil.NewTB(b)
	tb.Run(benchutil.OneSampleSeriesSubTestName(10e6), func(tb testutil.TB) {
		benchStoreSeriesSet(tb, 10e6, benchutil.SeriesDimension)
	})
	tb.Run(benchutil.OneSeriesManySamplesSubTestName(100e6), func(tb testutil.TB) {
		// 100e6 samples = ~17361 days with 15s scrape.
		benchStoreSeriesSet(tb, 100e6, benchutil.SamplesDimension)
	})
}

func benchStoreSeriesSet(t testutil.TB, number int, dimension benchutil.Dimension) {
	const numOfClients = 4

	var (
		numberPerClient = number / 4
		series          []storepb.Series
		lbls            = labels.FromStrings("ext1", "1", "foo", "bar", "i", postingsBenchSuffix)
		random          = rand.New(rand.NewSource(120))
	)
	switch dimension {
	case seriesDimension:
		series = make([]storepb.Series, 0, numOfClients*numberPerClient)
	case samplesDimension:
		series = []storepb.Series{newSeries(t, lbls, nil)}
	default:
		t.Fatal("unknown dimension", dimension)
	}

	var lset storepb.LabelSet
	for _, l := range lbls {
		lset.Labels = append(lset.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}

	// Build numOfClients of clients.
	clients := make([]Client, numOfClients)
	resps := make([]*storepb.SeriesResponse, numOfClients)
	for j := range clients {
		switch dimension {
		case seriesDimension:
			fmt.Println("Building client with numSeries:", numberPerClient)

			for _, s := range createSeriesWithOneSample(t, j, numberPerClient, nopAppender{}) {
				series = append(series, s)
				resps = append(resps, storepb.NewSeriesResponse(&s))
			}

			clients[j] = &testClient{
				StoreClient: &mockedStoreAPI{
					RespSeries: resps[j*numberPerClient : j*numberPerClient+numberPerClient],
				},
				labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext1", Value: "1"}}}},
				minTime:   int64(j * numberPerClient),
				maxTime:   series[len(series)-1].Chunks[len(series[len(series)-1].Chunks)-1].MaxTime,
			}
		case samplesDimension:
			fmt.Println("Building client with one series with numSamples:", numberPerClient)

			lblsSize := 0
			for _, l := range lset.Labels {
				lblsSize += l.Size()
			}

			c := chunkenc.NewXORChunk()
			a, err := c.Appender()
			testutil.Ok(t, err)

			sBytes := lblsSize
			lastResps := 0

			i := 0
			samples := 0
			nextTs := int64(j * numberPerClient)
			for {
				a.Append(int64(j*numberPerClient+i), random.Float64())
				i++
				samples++
				if i < numOfClients && samples < maxSamplesPerChunk {
					continue
				}

				series[0].Chunks = append(series[0].Chunks, storepb.AggrChunk{
					MinTime: nextTs,
					MaxTime: int64(j*numberPerClient + i - 1),
					Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
				})
				sBytes += len(c.Bytes())

				// Compose many frames as remote read would do (so sidecar StoreAPI): 1048576
				if i >= numOfClients || sBytes >= 1048576 {
					resps = append(resps, storepb.NewSeriesResponse(&storepb.Series{
						Labels: lset.Labels,
						Chunks: series[0].Chunks[lastResps:],
					}))
					lastResps = len(series[0].Chunks) - 1
				}
				if i >= numOfClients {
					break
				}

				nextTs = int64(j*numberPerClient + i)
			}

			clients[j] = &testClient{
				StoreClient: &mockedStoreAPI{
					RespSeries: resps[j*numberPerClient : j*numberPerClient+numberPerClient],
				},
				labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext1", Value: "1"}}}},
				minTime:   int64(j * numberPerClient),
				maxTime:   nextTs,
			}
		default:
			t.Fatal("unknown dimension", dimension)
		}
	}

	logger := log.NewNopLogger()
	store := &ProxyStore{
		logger:          logger,
		stores:          func() []Client { return clients },
		metrics:         newProxyStoreMetrics(nil),
		responseTimeout: 0,
	}

	maxTime := series[len(series)-1].Chunks[len(series[len(series)-1].Chunks)-1].MaxTime
	benchmarkSeries(t, store,
		&benchSeriesCase{
			name: fmt.Sprintf("%d of client with %d each, total %d", numOfClients, numberPerClient, number),
			req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: maxTime,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			expected: series,
		},
	)

	// Change client to just one.
	clients = clients[1:]
	clients[0] = &testClient{
		StoreClient: &mockedStoreAPI{
			// All responses.
			RespSeries: resps,
		},
		labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext1", Value: "1"}}}},
		minTime:   0,
		maxTime:   maxTime,
	}
	benchmarkSeries(t, store,
		&benchSeriesCase{
			name: fmt.Sprintf("single client with %d", number),
			req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: maxTime,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			expected: series,
		},
	)
}
