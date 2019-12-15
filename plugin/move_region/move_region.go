package main

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"go.uber.org/zap"
)

type moveRegionUserScheduler struct {
	*userBaseScheduler
	opController *schedule.OperatorController
	name         string
	startKey     []byte
	endKey       []byte
	storeIDs     []uint64
	startTime    time.Time
	endTime      time.Time
}

// Only use for register scheduler
// newMoveRegionUserScheduler() will be called manually
func init() {
	schedule.RegisterScheduler("move-region-user", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newMoveRegionUserScheduler(opController, "", []byte{}, []byte{}, []uint64{}, time.Time{}, time.Time{}), nil
	})
}

func newMoveRegionUserScheduler(opController *schedule.OperatorController, name string, startKey, endKey []byte, storeIDs []uint64, startTime, endTime time.Time) schedule.Scheduler {
	base := newUserBaseScheduler(opController)
	log.Info("", zap.String("New", name), zap.ByteStrings("key range", [][]byte{startKey, endKey}))
	return &moveRegionUserScheduler{
		userBaseScheduler: base,
		opController:      opController,
		name:              name,
		startKey:          startKey,
		endKey:            endKey,
		storeIDs:          storeIDs,
		startTime:         startTime,
		endTime:           endTime,
	}
}

func (r *moveRegionUserScheduler) GetName() string {
	return r.name
}

func (r *moveRegionUserScheduler) GetType() string {
	return "move-region-user"
}

func (r *moveRegionUserScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	currentTime := time.Now()
	if currentTime.Before(r.startTime) || currentTime.After(r.endTime) {
		return false
	}
	return r.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (r *moveRegionUserScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	// When region ids change, re-output scheduler's regions and stores
	regionIDs := schedule.GetRegionIDs(cluster, r.startKey, r.endKey)
	log.Info("", zap.String("Schedule()", r.GetName()), zap.Uint64s("Regions", regionIDs))
	// log.Info("", zap.String("Schedule()", r.GetName()), zap.Uint64s("Stores", r.storeIDs))

	if len(r.storeIDs) == 0 {
		return nil
	}
	/*
			filters := []filter.Filter{
				filter.StoreStateFilter{MoveRegion: true},
			}

			storeMap := make(map[uint64]struct{})
			storeIDs := []uint64{}
			// filter target stores first
			for _, storeID := range r.storeIDs {
				if !filter.Target(cluster, cluster.GetStore(storeID), filters) {
					storeMap[storeID] = struct{}{}
					storeIDs = append(storeIDs, storeID)
				}
			}

			if len(storeMap) == 0 {
				return nil
			}

			replicas := cluster.GetMaxReplicas()

		for _, regionID := range regionIDs {
			region := cluster.GetRegion(regionID)
			if region == nil {
				log.Info("region not exist", zap.Uint64("region-id", regionID))
				continue
			}
			// If filtered target stores all contain a region peer,
			// it means user's rules has been met,
			// then do nothing
			if !allExist(storeIDs, region) {
				// if region max-replicas > target stores length,
				// add the store where the original peer is located sequentially,
				// until target stores length = max-replicas
				for storeID := range region.GetStoreIds() {
					if replicas > len(storeMap) {
						if _, ok := storeMap[storeID]; !ok {
							if filter.Target(cluster, cluster.GetStore(storeID), filters) {
								log.Info("filter target", zap.String("scheduler", r.GetName()), zap.Uint64("store-id", storeID))
							} else {
								storeMap[storeID] = struct{}{}
							}
						}
					} else {
						break
					}
				}
				// if replicas still > target stores length, do nothing
				if replicas > len(storeMap) {
					log.Info("replicas > len(storeMap)", zap.String("scheduler", r.GetName()))
					continue
				}
				op, err := operator.CreateMoveRegionOperator(r.name, cluster, region, operator.OpAdmin, storeMap)
				if err != nil {
					log.Error("CreateMoveRegionOperator Err", zap.String("scheduler", r.GetName()), zap.Error(err))
					continue
				}
				return []*operator.Operator{op}
			}
		}
	*/
	for _, regionID := range regionIDs {
		region := cluster.GetRegion(regionID)
		if region == nil {
			log.Info("region not exist", zap.Uint64("region-id", regionID))
			continue
		}
		sourceID := region.GetLeader().GetStoreId()
		source := cluster.GetStore(sourceID)
		// If leader is in target stores,
		// it means user's rules has been met,
		// then do nothing
		if !isExists(sourceID, r.storeIDs) {
			// Let "seq" store be the target first
			targetID := r.storeIDs[0]
			target := cluster.GetStore(targetID)
			if _, ok := region.GetStoreIds()[targetID]; ok {
				// target store has region peer, so do "transfer leader"
				filters := []filter.Filter{
					filter.StoreStateFilter{TransferLeader: true},
				}
				if filter.Source(cluster, source, filters) {
					//log.Info("filter source", zap.String("scheduler", r.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("store-id", sourceID))
					continue
				}
				if filter.Target(cluster, target, filters) {
					//log.Info("filter target", zap.String("scheduler", r.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("store-id", targetID))
					continue
				}
				op := operator.CreateTransferLeaderOperator(r.name, region, sourceID, targetID, operator.OpLeader)
				op.SetPriorityLevel(core.HighPriority)
				return []*operator.Operator{op}
			} else {
				// target store doesn't have region peer, so do "move leader"
				filters := []filter.Filter{
					filter.StoreStateFilter{MoveRegion: true},
				}
				if filter.Source(cluster, source, filters) {
					//log.Info("filter source", zap.String("scheduler", r.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("store-id", sourceID))
					continue
				}
				if filter.Target(cluster, target, filters) {
					//log.Info("filter target", zap.String("scheduler", r.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("store-id", targetID))
					continue
				}
				destPeer, err := cluster.AllocPeer(targetID)
				if err != nil {
					log.Error("failed to allocate peer", zap.Error(err))
					continue
				}
				op, err := operator.CreateMoveLeaderOperator(r.name, cluster, region, operator.OpAdmin, sourceID, targetID, destPeer.GetId())
				if err != nil {
					log.Error("CreateMoveLeaderOperator Err",
						zap.String("scheduler", r.GetName()),
						zap.Error(err))
					continue
				}
				op.SetPriorityLevel(core.HighPriority)
				return []*operator.Operator{op}
			}
		}
	}
	return nil
}

// allExist(storeIDs, region) determine if all storeIDs contain a region peer
func allExist(storeIDs []uint64, region *core.RegionInfo) bool {
	for _, storeID := range storeIDs {
		if _, ok := region.GetStoreIds()[storeID]; ok {
			continue
		} else {
			return false
		}
	}
	return true
}

// isExists(ID , IDs) determine if the ID is in IDs
func isExists(ID uint64, IDs []uint64) bool {
	for _, id := range IDs {
		if id == ID {
			return true
		}
	}
	return false
}
