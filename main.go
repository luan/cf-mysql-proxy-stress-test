package main

import (
	"database/sql"
	"flag"
	"log"
	"sync"

	"github.com/Pallinder/go-randomdata"
	_ "github.com/go-sql-driver/mysql"
)

var dbString = flag.String("db", "", "sql connection string")

const (
	dbName         = "JenLikesCats"
	tableName      = "cats"
	rowsToSeed     = 1000
	maxConnections = 10
)

func main() {
	flag.Parse()
	dropDatabase()
	createDatabase()

	db, err := sql.Open("mysql", *dbString+dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(maxConnections)

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	createTable(db)
	seedDatabase(db)
}

func seedDatabase(db *sql.DB) {
	wg := sync.WaitGroup{}
	for i := 0; i < rowsToSeed; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Exec("INSERT INTO "+tableName+" (name, species) VALUES (?, ?)", randomdata.SillyName(), randomdata.SillyName())
			if err != nil {
				log.Println("failed to insert something", err)
			}
		}()
	}
	wg.Wait()
}

func createTable(db *sql.DB) {
	_, err := db.Exec("CREATE TABLE " + tableName + " (name VARCHAR(20), species VARCHAR(20) )")
	if err != nil {
		log.Fatal(err)
	}
}

func createDatabase() {
	db, err := sql.Open("mysql", *dbString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		log.Fatal(err)
	}
}

func dropDatabase() {
	db, err := sql.Open("mysql", *dbString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("DROP DATABASE " + dbName)
	if err != nil {
		log.Fatal(err)
	}
}
