package main

import (
	"fmt"
	"os"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
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
	conn, err := sqlite.OpenConn("herp.db")
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

func mainErr() error {
	cl, err := torrent.NewClient(nil)
	if err != nil {
		return err
	}
	defer cl.Close()
	ihs, err := getInfohashes()
	if err != nil {
		return fmt.Errorf("getting infohashes: %w", err)
	}
	for _, ih := range ihs {
		fmt.Printf("%s\n", ih.HexString())
	}
	return nil
}
