// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package retrieval

import (
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type nopAppendable struct{}

func (a nopAppendable) Appender() (storage.Appender, error) {
	return nopAppender{}, nil
}

type nopAppender struct{}

func (a nopAppender) Add(labels.Labels, int64, float64) (uint64, error) { return 0, nil }
func (a nopAppender) AddFast(uint64, int64, float64) error              { return nil }
func (a nopAppender) Commit() error                                     { return nil }
func (a nopAppender) Rollback() error                                   { return nil }

type collectResultAppender struct {
	refs   map[uint64]labels.Labels
	result []sample
}

func (a *collectResultAppender) SetSeries(l labels.Labels) (uint64, error) {
	if a.refs == nil {
		a.refs = map[uint64]labels.Labels{}
	}
	ref := uint64(len(a.refs))
	a.refs[ref] = l
	return ref, nil
}

func (a *collectResultAppender) Add(ref uint64, t int64, v float64) error {
	// for ln, lv := range s.Metric {
	// 	if len(lv) == 0 {
	// 		delete(s.Metric, ln)
	// 	}
	// }
	a.result = append(a.result, sample{
		metric: a.refs[ref],
		t:      t,
		v:      v,
	})
	return nil
}

func (a *collectResultAppender) Commit() error   { return nil }
func (a *collectResultAppender) Rollback() error { return nil }

// fakeTargetProvider implements a TargetProvider and allows manual injection
// of TargetGroups through the update channel.
type fakeTargetProvider struct {
	sources []string
	update  chan *config.TargetGroup
}

func (tp *fakeTargetProvider) Run(ch chan<- config.TargetGroup, done <-chan struct{}) {
	defer close(ch)
	for {
		select {
		case tg := <-tp.update:
			ch <- *tg
		case <-done:
			return
		}
	}
}

func (tp *fakeTargetProvider) Sources() []string {
	return tp.sources
}
