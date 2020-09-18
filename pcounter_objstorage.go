package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	pcounter "github.com/synerex/proto_pcounter"
	storage "github.com/synerex/proto_storage"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"

	sxutil "github.com/synerex/synerex_sxutil"
	//sxutil "local.packages/synerex_sxutil"

	"log"
	"sync"
)

// datastore provider provides Datastore Service.

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.01"
	baseDir         = "store"
	dataDir         string
	pcMu            *sync.Mutex = nil
	pcLoop          *bool       = nil
	ssMu            *sync.Mutex = nil
	ssLoop          *bool       = nil
	sxServerAddress string
	currentNid      uint64                  = 0 // NotifyDemand message ID
	mbusID          uint64                  = 0 // storage MBus ID
	storageID       uint64                  = 0 // storageID
	pc_client       *sxutil.SXServiceClient = nil
	st_client       *sxutil.SXServiceClient = nil
)

func init() {
}

func objStore(bc string, ob string, dt string) {

	log.Printf("Store %s, %s, %s", bc, ob, dt)
	//  we need to send data into mbusID.
	record := storage.Record{
		BucketName: bc,
		ObjectName: ob,
		Record:     []byte(dt),
	}
	out, err := proto.Marshal(&record)
	if err == nil && mbusID != 0 {
		cont := &api.Content{Entity: out}
		msg := &api.MbusMsg{
			MsgInfo:  "Store", // command
			TargetId: storageID,
			Cdata:    cont,
		}
		st_client.SendMbusMsg(context.Background(), msg)
	}

}

// called for each agent data.
func supplyPCounterCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	pc := &pcounter.PCounter{}

	err := proto.Unmarshal(sp.Cdata.Entity, pc)
	if err == nil { // get Pcounter
		ts0 := ptypes.TimestampString(pc.Ts)
		ld := fmt.Sprintf("%s,%s,%s,%s,%s", ts0, pc.Hostname, pc.Mac, pc.Ip, pc.IpVpn)

		tsd, _ := ptypes.Timestamp(pc.Ts)

		// how to define Bucket:

		bucketName := "pcounter"
		// we use IP address for sensor_id
		//		objectName := "sensor_id/year/month/date/hour/min/sec"
		objectName := fmt.Sprintf("%s/%4d/%02d/%02d/%02d/%02d/%02d", pc.Ip, tsd.Year(), tsd.Month(), tsd.Day(), tsd.Hour(), tsd.Minute(), tsd.Second())

		line := ld

		for _, ev := range pc.Data {
			ts := ptypes.TimestampString(ev.Ts)
			line += fmt.Sprintf("%s,%s,%d,%s,%s,", ts, pc.DeviceId, ev.Seq, ev.Typ, ev.Id)
			switch ev.Typ {
			case "counter":
				line = line + fmt.Sprintf("%s,%d", ev.Dir, ev.Height)
			case "fillLevel":
				line = line + fmt.Sprintf("%d", ev.FillLevel)
			case "dwellTime":
				tsex := ptypes.TimestampString(ev.TsExit)
				line = line + fmt.Sprintf("%f,%f,%s,%d,%d", ev.DwellTime, ev.ExpDwellTime, tsex, ev.ObjectId, ev.Height)
			}
			line += "\n"
		}
		objStore(bucketName, objectName, line)

	}
}

func subscribePCounterSupply(client *sxutil.SXServiceClient) {
	log.Printf("Start PCounter Supply")

	pcMu, pcLoop = sxutil.SimpleSubscribeSupply(client, supplyPCounterCallback)

}

func mbusCallback(clt *sxutil.SXServiceClient, mm *api.MbusMsg) {
	log.Printf("Mbus message %#v", mm)

	/*
		if sxutil.IDType(mm.TargetId) == clt.ClientID { // this msg is for me.
			if mm.MsgInfo == "Store" {
				record := &storage.Record{}
				err := proto.Unmarshal(mm.Cdata.Entity, record)
				if err == nil {
				}
			}
		}
	*/

}

func supplyStorageCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	//	log.Printf("Receive Supply! %v", sp)
	if sp.SupplyName == "Storage" {
		storageInfo := &storage.Storage{}
		// propose supply!?
		if sp.TargetId != 0 {
			if currentNid == sp.TargetId {
				// should check with previous one
				log.Printf("Receive ProposeSupply! %d %v", sp.TargetId, sp)
				err := proto.Unmarshal(sp.Cdata.Entity, storageInfo)
				if err == nil { /// lets start subscribe pcounter.
					// check handling function.
					if storageInfo.Stype == storage.StorageType_TYPE_OBJSTORE && storageInfo.Dtype == storage.DataType_DATA_FILE {
						log.Printf("Type OK")
						log.Printf("Send Select Supply! %v", sp)
						mbusID, err = clt.SelectSupply(sp)
						storageID = sp.SenderId
						if err != nil {
							log.Printf("SelectSupply err:%v", err)
							//				return
						} else {
							// start store with mbus.

							go clt.SubscribeMbus(context.Background(), mbusCallback)
							subscribePCounterSupply(pc_client)
						}
					} else {
						log.Printf("Unknown storage type/data %v", storageInfo)
					}
				} else {
					log.Printf("Cdata Content is broken %v", err)
				}
			}
		}
	}

}

func subscribeStorageSupply(client *sxutil.SXServiceClient) {

}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("PCounter-ObjStorage(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.PEOPLE_COUNTER_SVC, pbase.STORAGE_SERVICE}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "PCouterObjStorage", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	}

	st_client = sxutil.NewSXServiceClient(client, pbase.STORAGE_SERVICE, "{Client:PCObjStore}")
	pc_client = sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC, "{Client:PcountStore}")

	log.Print("Subscribe Storage Supply")
	ssMu, ssLoop = sxutil.SimpleSubscribeSupply(st_client, supplyStorageCallback)
	wg.Add(1)

	storageInfo := storage.Storage{
		Stype: storage.StorageType_TYPE_OBJSTORE,
		Dtype: storage.DataType_DATA_FILE,
	}

	out, err := proto.Marshal(&storageInfo)
	if err == nil {
		cont := api.Content{Entity: out}
		// Register supply
		dmo := sxutil.DemandOpts{
			Name:  "Storage",
			Cdata: &cont,
		}
		//			fmt.Printf("Res: %v",smo)
		//_, nerr :=
		currentNid, err = st_client.NotifyDemand(&dmo)
		if err == nil {
			log.Printf("Sending Notify Demand! %d", currentNid)
		} else {
			log.Printf("Notify Demend Send Error! %v", err)
		}
	}

	//	log.Print("Subscribe Supply")
	//	go subscribePCounterSupply(pc_client)

	wg.Wait()

}
