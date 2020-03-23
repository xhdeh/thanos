package benchutil

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

const (
	// LabelLongSuffix is a label with ~50B in size, to emulate real-world high cardinality.
	LabelLongSuffix = "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
)

type Dimension string

const (
	SeriesDimension  = Dimension("series")
	SamplesDimension = Dimension("samples")
)

var (
	SingleSeriesInternalLabels = labels.FromStrings("foo", "bar", "i", LabelLongSuffix)
	// Don't fill samples, would use too much memory.
	SingleSeries = NewTestSeries(nil, append(labels.FromStrings("ext1", "1"), SingleSeriesInternalLabels...), nil)
)

func OneSampleSeriesSubTestName(seriesNum int) string {
	return fmt.Sprintf("%dSeriesWithOneSample", seriesNum)
}

func OneSeriesManySamplesSubTestName(samplesNum int) string {
	return fmt.Sprintf("OneSeriesWith%dSamples", samplesNum)
}

func CreateSeriesWithOneSample(t testutil.TB, j int, totalSeries int) (*tsdb.Head, []storepb.Series) {
	fmt.Println("Creating one-sample series with numSeries:", totalSeries)

	h, err := tsdb.NewHead(nil, nil, nil, 1)
	testutil.Ok(t, err)

	app := h.Appender()
	series := make([]storepb.Series, totalSeries)
	var ts int64
	var lbls labels.Labels
	for i := 0; i < totalSeries; i++ {
		ts = int64(j*totalSeries + i)
		lbls = labels.FromStrings("foo", "bar", "i", fmt.Sprintf("%07d%s", ts, LabelLongSuffix))
		series[i] = NewTestSeries(t, append(labels.FromStrings("ext1", "1"), lbls...), sampleChunks([][]sample{{sample{t: ts, v: 0}}}))

		_, err := app.Add(lbls, ts, 0)
		testutil.Ok(t, err)
	}
	testutil.Ok(t, app.Commit())
	return h, series
}

func CreateOneSeriesWithManySamples(t testutil.TB, j int, totalSamples int, random *rand.Rand) *tsdb.Head {
	fmt.Println("Creating one series with numSamples:", totalSamples)

	h, err := tsdb.NewHead(nil, nil, nil, int64(totalSamples))
	testutil.Ok(t, err)

	app := h.Appender()

	ref, err := app.Add(SingleSeriesInternalLabels, int64(j*totalSamples), random.Float64())
	testutil.Ok(t, err)
	for i := 1; i < totalSamples; i++ {
		ts := int64(j*totalSamples + i)
		testutil.Ok(t, app.AddFast(ref, ts, random.Float64()))
	}
	testutil.Ok(t, app.Commit())
	return h
}

type Samples interface {
	Len() int
	Get(int) tsdbutil.Sample
}

type SampleChunks interface {
	Len() int
	Get(int) Samples
}

// sample struct is always copied as it is used very often, so we want to avoid long `benchutil.Sample` statements in tests.
type sample struct {
	t int64
	v float64
}

func (s sample) T() int64   { return s.t }
func (s sample) V() float64 { return s.v }

type samples []sample

func (s samples) Len() int { return len(s) }
func (s samples) Get(i int) tsdbutil.Sample {
	return s[i]
}

type sampleChunks [][]sample

func (c sampleChunks) Len() int { return len(c) }
func (c sampleChunks) Get(i int) Samples {
	return samples(c[i])
}

func NewTestSeries(t testing.TB, lset labels.Labels, smplChunks SampleChunks) storepb.Series {
	var s storepb.Series
	s.Labels = storepb.PromLabelsToLabels(lset)
	for i := 0; smplChunks != nil && i < smplChunks.Len(); i++ {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		testutil.Ok(t, err)

		for j := 0; j < smplChunks.Get(i).Len(); j++ {
			a.Append(smplChunks.Get(i).Get(j).T(), smplChunks.Get(i).Get(j).V())
		}

		ch := storepb.AggrChunk{
			MinTime: smplChunks.Get(i).Get(0).T(),
			MaxTime: smplChunks.Get(i).Get(smplChunks.Get(i).Len() - 1).T(),
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return s
}
