package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/anacrolix/envpprof"
	"github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
)

func main() {
	err := mainErr()
	if err != nil {
		log.Printf("error in main: %v", err)
		os.Exit(1)
	}
}

func getInfohashes() (ret []torrent.InfoHash, err error) {
	conn, err := getDatabaseConn()
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	defer conn.Close()
	err = sqlitex.Exec(
		conn,
		"select distinct infohash from sample_infohashes_response_infohash where hex(infohash) not in (select upper(infohash) from info)",
		func(stmt *sqlite.Stmt) error {
			var ih torrent.InfoHash
			if read := stmt.ColumnBytes(0, ih[:]); read != 20 {
				panic(read)
			}
			ret = append(ret, ih)
			return nil
		})
	if err != nil {
		err = fmt.Errorf("selecting infohashes: %w", err)
		return
	}
	return
}

func getDatabaseConn() (*sqlite.Conn, error) {
	return sqlite.OpenConn("herp.db")
}

func mainErr() error {
	defer envpprof.Stop()
	log.Default = log.Default.FilterLevel(log.Debug)
	cl, err := torrent.NewClient(nil)
	if err != nil {
		return err
	}
	defer cl.Close()
	http.HandleFunc("/torrentClientStatus", func(w http.ResponseWriter, r *http.Request) {
		cl.WriteStatus(w)
	})
	dbConn, err := getDatabaseConn()
	if err != nil {
		return fmt.Errorf("getting database conn: %w", err)
	}
	defer dbConn.Close()
	ihs, err := getInfohashes()
	if err != nil {
		return fmt.Errorf("getting infohashes: %w", err)
	}
	var wg sync.WaitGroup
	log.Printf("got %v infohashes", len(ihs))
	for _, ih := range ihs {
		t, _ := cl.AddTorrentInfoHash(ih)
		wg.Add(1)
		go func(ih torrent.InfoHash) {
			defer wg.Done()
			defer t.Drop()
			t.AddTrackers([][]string{{
				"udp://tracker.coppersurfer.tk:6969/announce",
				"http://tracker.opentrackr.org:1337/announce",
			}})
			started := time.Now()
			select {
			case <-t.GotInfo():
			case <-time.After(5 * time.Minute):
				log.Printf("timed out getting %v", ih)
				return
			}
			log.Printf("got info for %v (%v) after %v", ih, t, time.Since(started))
			err = sqlitex.Exec(dbConn,
				"insert or ignore into info(infohash, bytes) values (?, ?)", nil,
				ih.HexString(), []byte(t.Metainfo().InfoBytes))
			if err != nil {
				log.Printf("error inserting info into db: %v", err)
				return
			}
			log.Printf("got info bytes for %v", ih)
		}(ih)
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	noInfohashes := make(chan struct{})
	go func() {
		wg.Wait()
		close(noInfohashes)
	}()
	select {
	case <-interrupt:
		return errors.New("interrupted")
	case <-noInfohashes:
		return nil
	}
}
