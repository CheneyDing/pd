package schedulers

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"go.uber.org/zap"
	"sync"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(PredictRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*predictRegionSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			conf.Name = PredictRegionName
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
)

type predictRegionSchedulerConfig struct {
	Name   string          `json:"name"`
}

type predictScheduler struct {
	name string
	*BaseScheduler
	sync.RWMutex

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
	return ret
}

func (p *predictScheduler) GetName() string {
	return p.name
}

func (p *predictScheduler) GetType() string {
	return PredictRegionType
}

func (p *predictScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return p.OpController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (p *predictScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(p.GetName(), "schedule").Inc()
	// getTopK
	regions := cluster.GetRegions()
	regionIDs := getTopK(regions)
	log.Info("TopK", zap.Any("Region Count: ", len(regionIDs)))

	// find new store
	var newStores []*core.StoreInfo
	for _, store := range cluster.GetStores() {
		if (store.GetLabelValue(filter.SpecialUseKey) == filter.SpecialUseHotRegion) && !store.IsOffline() {
			newStores = append(newStores, store)
		}
	}

	if len(newStores) == 0 {
		log.Info("New store is nil")
		return nil
	}
	log.Info("New stores", zap.Any("Stores:", newStores))

	// check topK region
	for _, regionID := range regionIDs {
		region := cluster.GetRegion(regionID)
		stores := cluster.GetRegionStores(region)
		isOK := false
		for _, store := range stores {
			if store.GetLabelValue(filter.SpecialUseKey) == filter.SpecialUseHotRegion {
				if region.GetLeader().GetStoreId() == store.GetID() {
					isOK = true
					break
				} else {
					// transfer leader to new store
					op, err := operator.CreateTransferLeaderOperator(PredictRegionType, cluster, region, region.GetLeader().GetStoreId(), store.GetID(), operator.OpLeader)
					if err != nil {
						log.Debug("fail to create predict region operator", zap.Error(err))
						return nil
					}
					return []*operator.Operator{op}
				}
			}
		}
		if !isOK {
			// move leader to new store
			destStoreID := pickDestStore(newStores)
			destPeer := &metapb.Peer{StoreId: destStoreID}
			op, err := operator.CreateMoveLeaderOperator("predict-move-leader", cluster, region, operator.OpRegion|operator.OpLeader, region.GetLeader().GetStoreId(), destPeer)
			if err != nil {
				log.Debug("fail to create move leader operator", zap.Error(err))
				return nil
			}
			return []*operator.Operator{op}
		}
	}

	return nil
}

func pickDestStore(stores []*core.StoreInfo) uint64 {
	minCount := stores[0].GetLeaderCount()
	minStoreID := stores[0].GetID()
	for _, store := range stores {
		if store.GetLeaderCount() < minCount {
			minCount = store.GetLeaderCount()
			minStoreID = store.GetID()
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
	topk := len(regions) / 4
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




