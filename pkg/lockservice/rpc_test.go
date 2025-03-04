// Copyright 2022 Matrix Origin
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

package lockservice

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					LockTable: lock.LockTable{ServiceID: "s1"},
					Method:    lock.Method_Lock})
			require.NoError(t, err)
			assert.NotNil(t, resp)
		},
	)
}

func TestMOErrorCanHandled(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, moerr.NewDeadLockDetectedNoCtx(), cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.Error(t, err)
			require.Nil(t, resp)
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected))
		},
	)
}

func TestRequestCanBeFilter(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.Error(t, err)
			require.Nil(t, resp)
			require.Equal(t, err, ctx.Err())
		},
		WithServerMessageFilter(func(r *lock.Request) bool { return false }),
	)
}

func TestLockTableBindChanged(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &lock.LockTable{ServiceID: "s1"}
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.NoError(t, err)
			require.NotNil(t, resp.NewBind)
			assert.Equal(t, lock.LockTable{ServiceID: "s1"}, *resp.NewBind)
		},
	)
}

func runRPCTests(
	t *testing.T,
	fn func(Client, Server),
	opts ...ServerOption) {
	defer leaktest.AfterTest(t)()
	testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	assert.NoError(t, os.RemoveAll(testSockets[7:]))

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			[]metadata.CNService{
				{
					ServiceID:          "s1",
					LockServiceAddress: testSockets,
				},
				{
					ServiceID:          "s2",
					LockServiceAddress: testSockets,
				},
			},
			[]metadata.DNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	s, err := NewServer(testSockets, morpc.Config{}, opts...)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, s.Close())
	}()
	require.NoError(t, s.Start())

	c, err := NewClient(morpc.Config{})
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	fn(c, s)
}
