package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/anacrolix/envpprof"
	"github.com/anacrolix/torrent"
)

func main() {
	err := mainErr()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error in main: %v", err)
		os.Exit(1)
	}
}

func getInfohashes() (ret []torrent.InfoHash, err error) {
	conn, err := getDatabaseConn()
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	defer conn.Close()
	err = sqlitex.Exec(conn, "select distinct infohash from sample_infohashes_response_infohash where infohash not in (select infohash from info)", func(stmt *sqlite.Stmt) error {
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
	cl, err := torrent.NewClient(nil)
	if err != nil {
		return err
	}
	defer cl.Close()
	http.HandleFunc("/torrentClientStatus", func(w http.ResponseWriter, r *http.Request) {
		cl.WriteStatus(w)
	})
	ihs, err := getInfohashes()
	if err != nil {
		return fmt.Errorf("getting infohashes: %w", err)
	}
	var wg sync.WaitGroup
	for _, ih := range ihs {
		t, _ := cl.AddTorrentInfoHash(ih)
		wg.Add(1)
		go func(ih torrent.InfoHash) {
			defer t.Drop()
			<-t.GotInfo()
			conn, err := getDatabaseConn()
			if err != nil {
				log.Printf("error getting database conn: %v", err)
				return
			}
			defer conn.Close()
			err = sqlitex.Exec(conn, "insert or ignore into info(infohash, bytes) values (?, ?)", nil, ih, []byte(t.Metainfo().InfoBytes))
			if err != nil {
				log.Printf("error inserting info into db: %v", err)
				return
			}
			log.Printf("got info bytes for %v", ih)
		}(ih)
	}
	wg.Wait()
	return nil
}
