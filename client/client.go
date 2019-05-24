package client

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
	cconfig "github.com/sdojjy/tikv-coprocessor-client/config"
	"github.com/sdojjy/tikv-coprocessor-client/txnkv"
	"strings"

	//"github.com/pingcap/log"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	kvstore "github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"time"
)

const (
	readTimeout = 20 * time.Second
)

// Client is a client that sends RPC.
type ClusterClient struct {
	PdClient    pd.Client
	RpcClient   *rpcClient
	RegionCache *tikv.RegionCache
	Storage     kv.Storage
}

type RegionMeta struct {
	Region *metapb.Region
	Peer   *metapb.Peer
}

// NewRawKVClient creates a client with PD cluster addrs.
func NewClient(pdAddrs []string, security config.Security) (*ClusterClient, error) {
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	})
	if err != nil {
		return nil, err
	}

	kvstore.Register("tikv", tikv.Driver{})
	fullPath := fmt.Sprintf(fmt.Sprintf("tikv://%s?disableGC=true", strings.Join(pdAddrs, ",")))
	storage, err := kvstore.New(fullPath)

	if err != nil {
		return nil, err
	}

	pClid := &codecPDClient{pdCli}
	return &ClusterClient{
		PdClient:    pClid,
		RegionCache: tikv.NewRegionCache(pClid),
		RpcClient:   newRPCClient(security),
		Storage:     storage,
	}, nil
}

func (c *ClusterClient) GetRegionInfo(ctx context.Context, id uint64) (*tikv.KeyLocation, error) {
	return c.RegionCache.LocateRegionByID(NewBackOffer(ctx), id)
}

func NewBackOffer(ctx context.Context) *tikv.Backoffer {
	return tikv.NewBackoffer(ctx, 20000)
}

func (c *ClusterClient) GetRegion(id uint64) (*RegionMeta, error) {
	r, peer, err := c.PdClient.GetRegionByID(getContext(), id)
	if err != nil {
		return nil, err
	}

	return &RegionMeta{
		Region: r,
		Peer:   peer,
	}, nil
}

func getContext() context.Context {
	return context.Background()
}

func (c *ClusterClient) loadStoreAddr(ctx context.Context, bo *tikv.Backoffer, id uint64) (string, error) {
	for {
		store, err := c.PdClient.GetStore(ctx, id)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return "", err
			}
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", id, err)
			if err = bo.Backoff(tikv.BoPDRPC, err); err != nil {
				return "", errors.Trace(err)
			}
			continue
		}
		if store == nil {
			return "", nil
		}
		return store.GetAddress(), nil
	}
}

func (c *ClusterClient) GetTableInfo(dbName, tableName string) (*model.TableInfo, error) {
	schema, err := c.Schema()
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tableVal.Meta(), nil
}

func (c *ClusterClient) Schema() (infoschema.InfoSchema, error) {
	sx, err := session.CreateSession(c.Storage)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return domain.GetDomain(sx.(sessionctx.Context)).InfoSchema(), nil
}

func (c *ClusterClient) GetTableRegion(tableID int64) (*server.TableRegions, error) {
	schema, err := c.Schema()
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl, ok := schema.TableByID(tableID)
	if !ok {
		return nil, errors.New("table is not found")
	}
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	recordRegionIDs, err := c.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	recordRegions, err := c.getRegionsMeta(recordRegionIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for indices
	indices := make([]server.IndexRegions, len(tbl.Indices()))
	for i, index := range tbl.Indices() {
		indexID := index.Meta().ID
		indices[i].Name = index.Meta().Name.String()
		indices[i].ID = indexID
		startKey, endKey := tablecodec.GetTableIndexKeyRange(tableID, indexID)
		rIDs, err := c.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		indices[i].Regions, err = c.getRegionsMeta(rIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	tableRegions := &server.TableRegions{
		TableName:     tbl.Meta().Name.O,
		TableID:       tableID,
		Indices:       indices,
		RecordRegions: recordRegions,
	}
	return tableRegions, nil

}

func (c *ClusterClient) GetRecordRegionIds(tableID int64) ([]uint64, error) {
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	return c.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
}

func (c *ClusterClient) GetIndexRegionIds(tableId, idxId int64) (regionIDs []uint64, err error) {
	startKey, endKey := tablecodec.GetTableIndexKeyRange(tableId, idxId)
	return c.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
}

// Put stores a key-value pair to TiKV.
func (c *ClusterClient) Put(key, value []byte) error {
	cli, err := txnkv.NewClient([]string{"127.0.0.1:2379"}, cconfig.Default())
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	tx, err := cli.Begin()
	if err != nil {
		return err
	}

	err = tx.Set(key, value)
	if err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

func (c *ClusterClient) getRegionsMeta(regionIDs []uint64) ([]server.RegionMeta, error) {
	regions := make([]server.RegionMeta, len(regionIDs))
	for i, regionID := range regionIDs {
		meta, leader, err := c.RegionCache.PDClient().GetRegionByID(context.TODO(), regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if meta == nil {
			return nil, errors.Errorf("region not found for regionID %q", regionID)
		}
		regions[i] = server.RegionMeta{
			ID:          regionID,
			Leader:      leader,
			Peers:       meta.Peers,
			RegionEpoch: meta.RegionEpoch,
		}

	}
	return regions, nil
}
