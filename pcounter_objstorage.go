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

	"log"
	"sync"
	"time"
)

// datastore provider provides Datastore Service.

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.01"
	baseDir         = "store"
	dataDir         string
	sxServerAddress string
	mbusID          uint64                  = 0 // storage MBus ID
	pc_client       *sxutil.SXServiceClient = nil
)

func init() {
}

func objStore(bc string, ob string, dt string) {

	log.Printf("Store %s, %s, %s", bc, ob, dt)
}

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

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
	}
	mu.Unlock()
}

func subscribePCounterSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyPCounterCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func supplyStorageCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	storageInfo := &storage.Storage{}

	if sp.SupplyName == "Storage" {
		err := proto.Unmarshal(sp.Cdata.Entity, storageInfo)
		if err == nil { /// lets start subscribe pcounter.
			log.Printf("Send Select Supply! %v", sp)
			mbusID, _ = clt.SelectSupply(sp)
			//			if sserr != nil {
			//				mbusID = -1
			//				return
			//			}
			// start store with mbus.
			go subscribePCounterSupply(pc_client)
		}
	}

}

func subscribeStorageSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyStorageCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
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
	} else {
		log.Print("Connecting SynerexServer")
	}

	st_client := sxutil.NewSXServiceClient(client, pbase.STORAGE_SERVICE, "{Client:PCObjStore}")
	//	pc_client = sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC, "{Client:PcountStore}")

	log.Print("Subscribe Storage Supply")
	go subscribeStorageSupply(st_client)

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
		log.Printf("Sending Notify Demend!")
		st_client.NotifyDemand(&dmo)
	}

	log.Printf("umm")

	wg.Add(1)
	//	log.Print("Subscribe Supply")
	//	go subscribePCounterSupply(pc_client)

	wg.Wait()

}
