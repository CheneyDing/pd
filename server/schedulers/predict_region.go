package schedulers

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
	"math"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(PredictRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler(PredictRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &predictRegionSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.Name = PredictRegionName
		return newPredictScheduler(opController, conf), nil
	})
}

const (
	// PredictRegionName is balance region scheduler name.
	PredictRegionName = "predict-region-scheduler"
	// PredictRegionType is balance region scheduler type.
	PredictRegionType = "predict-region"

	predictRegionTopK = 0.05
)

type predictRegionSchedulerConfig struct {
	Name   string          `json:"name"`
}

type predictScheduler struct {
	name string
	*BaseScheduler
	filters      []filter.Filter
	// config of predict scheduler
	conf *predictRegionSchedulerConfig
}

func newPredictScheduler(opController *schedule.OperatorController, conf *predictRegionSchedulerConfig) *predictScheduler {
	base := NewBaseScheduler(opController)
	ret := &predictScheduler{
		name:           PredictRegionName,
		BaseScheduler:  base,
		conf:           conf,
	}
	ret.filters = []filter.Filter{
		filter.StoreStateFilter{ActionScope: PredictRegionType, TransferLeader: true, MoveRegion: true},
	}
	return ret
}

func (p *predictScheduler) GetName() string {
	return p.name
}

func (p *predictScheduler) GetType() string {
	return PredictRegionType
}

func (p *predictScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return p.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit() &&
		p.OpController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit()
}

func (p *predictScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(p.GetName(), "schedule").Inc()
	// getTopK
	regions := cluster.GetRegions()
	regionIDs := getTopK(regions)
	//log.Info("TopK", zap.Any("Region Count: ", len(regionIDs)))

	// find new store
	newStores := make(map[uint64]*core.StoreInfo)
	for _, store := range cluster.GetStores() {
		if store.GetLabelValue(filter.SpecialUseKey) == filter.SpecialUseHotRegion &&
			filter.Target(cluster.GetOpts(), store, p.filters) {
			newStores[store.GetID()] = store
		}
	}

	if len(newStores) == 0 {
		log.Info("New store is nil")
		return nil
	}
	//log.Info("New stores", zap.Any("Stores:", newStores))

	// check topK region
	for _, regionID := range regionIDs {
		region := cluster.GetRegion(regionID)
		// filter out new stores with out region's peer
		stores := region.GetStoreIds()
		availNewStores := make(map[uint64]*core.StoreInfo)
		for _, store := range newStores {
			if _, ok := stores[store.GetID()]; !ok{
				availNewStores[store.GetID()] = store
			}
		}
		//log.Info("Avail new stores", zap.Any("Stores:", availNewStores))
		// priority process leader
		leader := region.GetLeader()
		if _, ok := newStores[leader.GetStoreId()]; !ok {
			if len(availNewStores) == 0{
				destStoreID := pickDestStore(cluster, newStores)
				destStore := cluster.GetStore(destStoreID)
				if float64(destStore.GetLeaderCount()) * 1.05 < predictRegionTopK * float64(cluster.GetRegionCount()) {
					op, err := operator.CreateTransferLeaderOperator("predict-transfer-leader", cluster, region, region.GetLeader().GetStoreId(), destStoreID, operator.OpLeader)
					if err != nil {
						log.Debug("fail to create predict region operator", zap.Error(err))
						return nil
					}
					return []*operator.Operator{op}
				}
			} else {
				destStoreID := pickDestStore(cluster, availNewStores)
				destStore := cluster.GetStore(destStoreID)
				if float64(destStore.GetLeaderCount()) * 1.05 < predictRegionTopK * float64(cluster.GetRegionCount()) {
					destPeer := &metapb.Peer{StoreId: destStoreID}
					op, err := operator.CreateMoveLeaderOperator("predict-move-leader", cluster, region, operator.OpRegion|operator.OpLeader, region.GetLeader().GetStoreId(), destPeer)
					if err != nil {
						log.Debug("fail to create move leader operator", zap.Error(err))
						return nil
					}
					return []*operator.Operator{op}
				}
			}
		}
		// then process other peers
		peers := region.GetPeers()
		for _, peer := range peers {
			_, ok := newStores[peer.GetStoreId()]
			if peer.GetId() != region.GetLeader().GetId() && !ok && len(availNewStores) != 0{
				destStoreID := pickDestStore(cluster, availNewStores)
				destStore := cluster.GetStore(destStoreID)
				if float64(destStore.GetRegionCount()) * 1.05 < predictRegionTopK * 3 * float64(cluster.GetRegionCount())  {
					dstPeer := &metapb.Peer{StoreId: destStoreID, Role: peer.Role}
					op, err := operator.CreateMovePeerOperator("predict-move-peer", cluster, region, operator.OpRegion, peer.GetStoreId(), dstPeer)
					if err != nil {
						log.Debug("fail to create move peer operator", zap.Error(err))
						return nil
					}
					return []*operator.Operator{op}
				}
			}
		}
	}

	return nil
}

func pickDestStore(cluster opt.Cluster, stores map[uint64]*core.StoreInfo) uint64 {
	minCount := uint64(math.MaxUint64)
	minStoreID := uint64(math.MaxUint64)
	for id, store := range stores {
		if uint64(store.GetRegionCount()) < minCount {
			minCount = uint64(store.GetRegionCount())
			minStoreID = id
		}
	}
	return minStoreID
}

type HotRegionTable struct {
	count    uint64
	regionID []uint64
}

func getTopK(regions []*core.RegionInfo) []uint64 {
	//log.Info("Start getTopK")
	if len(regions) <= 0 {
		log.Info("not found region")
		return []uint64{}
	}
	var minrw float64 = -1
	var maxrw float64 = -1
	tmp := make([]float64, len(regions))
	for index, v := range regions {
		tmpSize := v.GetApproximateSize() //maybe 0
		if tmpSize == 0 {
			tmpSize = 1
		}
		tmp[index] = float64(v.GetRwBytesTotal()) / float64(tmpSize)
		if (minrw > tmp[index]) || (minrw == -1) {
			minrw = tmp[index]
		}
		if maxrw < tmp[index] {
			maxrw = tmp[index]
		}
	}
	segment := (maxrw - minrw) / float64(len(regions))
	if segment == 0 {
		segment = 1
	}
	HotDegree := make([]HotRegionTable, len(regions)+10)
	for index, v := range regions {
		data := tmp[index]
		indexData := (maxrw - data) / segment
		if int(indexData) > len(regions) {
			log.Info("maxrw: ", zap.Any("maxrw", maxrw))
			log.Info("minrw: ", zap.Any("minrw", minrw))
			log.Info("segment: ", zap.Any("segment", segment))
			log.Info("len of HotDegree: ", zap.Any("len(HotDegree)", len(HotDegree)))
			log.Info("data: ", zap.Any("data", data))
			log.Info("indexData: ", zap.Any("indexData", indexData))
		}
		HotDegree[int(indexData)].count++
		HotDegree[int(indexData)].regionID = append(HotDegree[int(indexData)].regionID, v.GetID())
	}
	k := 0
	topk := int(float64(len(regions)) * predictRegionTopK)
	var retRegionID []uint64
LOOP:
	for _, h := range HotDegree {
		for _, i := range h.regionID {
			retRegionID = append(retRegionID, i)
			k++
			if k == topk {
				break LOOP
			}
		}
	}
	//log.Info("GetTopK", zap.Any("TopK regionIDs", retRegionID))
	//log.Info("GetTopK", zap.Any("Len of TopK regionIDs", len(retRegionID)))
	return retRegionID
}

func isExists(ID uint64, IDs []uint64) bool {
	for _, id := range IDs {
		if id == ID {
			return true
		}
	}
	return false
}




