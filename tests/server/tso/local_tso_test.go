// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tso_test

import (
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

var _ = Suite(&testLocalTSOSuite{})

type testLocalTSOSuite struct {
	ctx         context.Context
	cancel      context.CancelFunc
	tsPoolMutex sync.Mutex
	tsPool      map[uint64]struct{}
}

func (s *testLocalTSOSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.tsPool = make(map[uint64]struct{})
	server.EnableZap = true
}

func (s *testLocalTSOSuite) TearDownSuite(c *C) {
	s.cancel()
}

// TestNormalGlobalTSO is used to test the normal way of global TSO generation.
func (s *testLocalTSOSuite) TestLocalTSO(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.LocalTSO.EnableLocalTSO = true
		conf.LocalTSO.DCLocation = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	leaderServer := cluster.GetServer(cluster.GetLeader())
	dcClientMap := make(map[string]pdpb.PDClient)
	for _, dcLocation := range dcLocationConfig {
		pdName := leaderServer.GetAllocatorLeader(dcLocation).GetName()
		dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lastList := make(map[string]*pdpb.Timestamp)
			for _, dcLocation := range dcLocationConfig {
				lastList[dcLocation] = &pdpb.Timestamp{
					Physical: 0,
					Logical:  0,
				}
			}
			for j := 0; j < 30; j++ {
				for _, dcLocation := range dcLocationConfig {
					req := &pdpb.TsoRequest{
						Header:     testutil.NewRequestHeader(leaderServer.GetClusterID()),
						Count:      tsoCount,
						DcLocation: dcLocation,
					}
					ts := testGetLocalTimestamp(c, dcClientMap[dcLocation], req)
					lastTS := lastList[dcLocation]
					// Check whether the TSO fallbacks
					c.Assert(tsoutil.CompareTimestamp(ts, lastTS), Equals, 1)
					lastList[dcLocation] = ts
					// Check whether the TSO is not unique
					c.Assert(s.checkTSOUnique(ts), IsTrue)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func testGetLocalTimestamp(c *C, pdCli pdpb.PDClient, req *pdpb.TsoRequest) *pdpb.Timestamp {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsoClient, err := pdCli.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, req.GetCount())
	res := resp.GetTimestamp()
	c.Assert(res.GetPhysical(), Greater, int64(0))
	c.Assert(res.GetLogical(), GreaterEqual, int64(req.GetCount()))
	return res
}

func (s *testLocalTSOSuite) checkTSOUnique(tso *pdpb.Timestamp) bool {
	s.tsPoolMutex.Lock()
	defer s.tsPoolMutex.Unlock()
	ts := tsoutil.GenerateTS(tso)
	if _, exist := s.tsPool[ts]; exist {
		return false
	}
	s.tsPool[ts] = struct{}{}
	return true
}

func (s *testLocalTSOSuite) TestLocalTSOAfterMemberChanged(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.LocalTSO.EnableLocalTSO = true
		conf.LocalTSO.DCLocation = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	leaderServer := cluster.GetServer(cluster.GetLeader())
	leaderCli := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(cluster.GetCluster().GetId()),
		Count:      tsoCount,
		DcLocation: config.GlobalDCLocation,
	}
	globalTS := testGetLocalTimestamp(c, leaderCli, req)

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Mock the situation that the system time of PD nodes in dc-4 is slower than others.
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/systemTimeSlow", `return(true)`), IsNil)

	// Join a new dc-location
	pd4, err := cluster.Join(s.ctx, func(conf *config.Config, serverName string) {
		conf.LocalTSO.EnableLocalTSO = true
		conf.LocalTSO.DCLocation = "dc-4"
	})
	c.Assert(err, IsNil)
	err = pd4.Run()
	c.Assert(err, IsNil)
	dcLocationConfig["pd4"] = "dc-4"
	cluster.CheckClusterDCLocation()
	testutil.WaitUntil(c, func(c *C) bool {
		leaderName := cluster.WaitAllocatorLeader("dc-4")
		return leaderName != ""
	})

	dcClientMap := make(map[string]pdpb.PDClient)
	for _, dcLocation := range dcLocationConfig {
		pdName := leaderServer.GetAllocatorLeader(dcLocation).GetName()
		dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lastList := make(map[string]*pdpb.Timestamp)
			for _, dcLocation := range dcLocationConfig {
				lastList[dcLocation] = &pdpb.Timestamp{
					Physical: 0,
					Logical:  0,
				}
			}
			for j := 0; j < 30; j++ {
				for _, dcLocation := range dcLocationConfig {
					req := &pdpb.TsoRequest{
						Header:     testutil.NewRequestHeader(cluster.GetCluster().GetId()),
						Count:      tsoCount,
						DcLocation: dcLocation,
					}
					ts := testGetLocalTimestamp(c, dcClientMap[dcLocation], req)
					lastTS := lastList[dcLocation]
					// Check whether the TSO fallbacks
					c.Assert(tsoutil.CompareTimestamp(ts, lastTS), Equals, 1)
					// Because we have a Global TSO synchronization, even though the system time
					// of the PD nodes in dc-4 is slower, its TSO will still be big enough.
					c.Assert(tsoutil.CompareTimestamp(ts, globalTS), Equals, 1)
					lastList[dcLocation] = ts
					// Check whether the TSO is not unique
					c.Assert(s.checkTSOUnique(ts), IsTrue)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	failpoint.Disable("github.com/tikv/pd/server/tso/systemTimeSlow")
}
