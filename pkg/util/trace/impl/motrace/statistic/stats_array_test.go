// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistic

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewStatsArray(t *testing.T) {
	type field struct {
		version      float64
		timeConsumed float64
		memory       float64
		s3in         float64
		s3out        float64
	}
	tests := []struct {
		name       string
		field      field
		wantString []byte
	}{
		{
			name: "normal",
			field: field{
				version:      1,
				timeConsumed: 2,
				memory:       3,
				s3in:         4,
				s3out:        5,
			},
			wantString: []byte(`[1,2,3.000,4,5]`),
		},
		{
			name: "random",
			field: field{
				version:      123,
				timeConsumed: 65,
				memory:       6,
				s3in:         78,
				s3out:        3494,
			},
			wantString: []byte(`[123,65,6.000,78,3494]`),
		},
		{
			name: "random_with_0",
			field: field{
				version:      123,
				timeConsumed: 65,
				memory:       0,
				s3in:         7834,
				s3out:        0,
			},
			wantString: []byte(`[123,65,0,7834,0]`),
		},
		{
			name: "scale_3",
			field: field{
				version:      123,
				timeConsumed: 65,
				memory:       0.001,
				s3in:         7834,
				s3out:        0,
			},
			wantString: []byte(`[123,65,0.001,7834,0]`),
		},
		{
			name: "scale_0.00049",
			field: field{
				version:      123,
				timeConsumed: 65,
				memory:       0.0001,
				s3in:         7834,
				s3out:        0,
			},
			wantString: []byte(`[123,65,0.000,7834,0]`),
		},
		{
			name: "scale_0.0005",
			field: field{
				version:      123,
				timeConsumed: 65,
				memory:       0.0005,
				s3in:         7834,
				s3out:        0,
			},
			wantString: []byte(`[123,65,0.001,7834,0]`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStatsArray().
				WithVersion(tt.field.version).
				WithTimeConsumed(tt.field.timeConsumed).
				WithMemorySize(tt.field.memory).
				WithS3IOInputCount(tt.field.s3in).
				WithS3IOOutputCount(tt.field.s3out)
			got := s.ToJsonString()
			require.Equal(t, tt.wantString, got)
			require.Equal(t, tt.field.version, s.GetVersion())
			require.Equal(t, tt.field.timeConsumed, s.GetTimeConsumed())
			require.Equal(t, tt.field.memory, s.GetMemorySize())
			require.Equal(t, tt.field.s3in, s.GetS3IOInputCount())
			require.Equal(t, tt.field.s3out, s.GetS3IOOutputCount())
		})
	}
}

func TestStatsArray_Add(t *testing.T) {
	type field struct {
		version      float64
		timeConsumed float64
		memory       float64
		s3in         float64
		s3out        float64
	}
	tests := []struct {
		name       string
		field      field
		wantString []byte
	}{
		{
			name: "normal",
			field: field{
				version:      1,
				timeConsumed: 2,
				memory:       3,
				s3in:         4,
				s3out:        5,
			},
			wantString: []byte(`[1,3,5.000,7,9]`),
		},
		{
			name: "random",
			field: field{
				version:      123,
				timeConsumed: 65,
				memory:       6,
				s3in:         78,
				s3out:        3494,
			},
			wantString: []byte(`[1,66,8.000,81,3498]`),
		},
	}

	getInitStatsArray := func() *StatsArray {
		return NewStatsArray().WithTimeConsumed(1).WithMemorySize(2).WithS3IOInputCount(3).WithS3IOOutputCount(4)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := getInitStatsArray()
			s := NewStatsArray().
				WithVersion(tt.field.version).
				WithTimeConsumed(tt.field.timeConsumed).
				WithMemorySize(tt.field.memory).
				WithS3IOInputCount(tt.field.s3in).
				WithS3IOOutputCount(tt.field.s3out)
			got := dst.Add(s).ToJsonString()
			require.Equal(t, tt.wantString, got)
		})
	}
}
