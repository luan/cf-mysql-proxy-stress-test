package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Pallinder/go-randomdata"
	_ "github.com/go-sql-driver/mysql"

	. "github.com/onsi/gomega"
)

var dbString = flag.String("db", "", "sql connection string")

var scenario = flag.Int("scenario", 1, "which scenario to run")

var maxConnections = flag.Int("maxConnections", 10, "max number of connections to the DB")

var rowsToSeed = flag.Int("rowsToSeed", 1000, "number of rows to seed the database with")

var dbName = flag.String("dbName", "cfMysqlStressTestDb", "database to create/drop for the test")

type Cat struct {
	Name    string
	Species string
}

func failHandler(message string, callerSkip ...int) {
	log.Fatal(message)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	RegisterFailHandler(failHandler)

	flag.Parse()
	dropDatabase(*dbName)
	createDatabase(*dbName)
	defer dropDatabase(*dbName)

	db, err := sql.Open("mysql", *dbString+*dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(*maxConnections)

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	createTable(db)
	seedDatabase(db, *rowsToSeed)

	initialCats := read(db)
	totalCats := int64(len(initialCats))

	fmt.Printf("running scenario %d\n", *scenario)
	switch *scenario {
	case 1:
		dropLeader()
	case 2:
		reads(db, 1, &totalCats)
	case 3:
		writes(db, 1, &totalCats)
	case 4:
		reads(db, *maxConnections, &totalCats)
	case 5:
		writes(db, *maxConnections, &totalCats)
	}

	finalCats := read(db)
	Expect(finalCats).To(HaveLen(int(totalCats)))
}

func reads(db *sql.DB, parallelism int, totalCatsPtr *int64) {
	stop := make(chan struct{})
	wg := sync.WaitGroup{}

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					cats := read(db)
					totalCats := atomic.LoadInt64(totalCatsPtr)
					Expect(cats).To(HaveLen(int(totalCats)))
				}
			}
		}()
	}

	dropLeader()
	close(stop)
	wg.Wait()
}

func writes(db *sql.DB, parallelism int, totalCatsPtr *int64) {
	stop := make(chan struct{})
	wg := sync.WaitGroup{}

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					insertCat(db)
					atomic.AddInt64(totalCatsPtr, 1)
				}
			}
		}()
	}

	dropLeader()
	close(stop)
	wg.Wait()
}

func read(db *sql.DB) []Cat {
	cats := []Cat{}
	rows, err := db.Query("SELECT name, species FROM cats ORDER BY id")
	if err != nil {
		log.Println("failed read:", err)
	}

	for rows.Next() {
		var cat Cat
		err := rows.Scan(&cat.Name, &cat.Species)
		if err != nil {
			log.Fatal(err)
		}
		cats = append(cats, cat)
	}

	return cats
}

func dropLeader() {
	fmt.Print("Kill the leader now and press ENTER")

	reader := bufio.NewReader(os.Stdin)
	reader.ReadString('\n')
}

func seedDatabase(db *sql.DB, numRows int) {
	wg := sync.WaitGroup{}
	for i := 0; i < numRows; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			insertCat(db)
		}()
	}
	wg.Wait()
}

func insertCat(db *sql.DB) Cat {
	cat := Cat{randomdata.SillyName(), randomdata.SillyName()}
	_, err := db.Exec("INSERT INTO cats (name, species) VALUES (?, ?)", cat.Name, cat.Species)
	if err != nil {
		log.Println("failed write:", err)
	}
	return cat
}

func createTable(db *sql.DB) {
	_, err := db.Exec("CREATE TABLE cats (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(20), species VARCHAR(20) )")
	if err != nil {
		log.Fatal(err)
	}
}

func createDatabase(dbName string) {
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

func dropDatabase(dbName string) {
	db, err := sql.Open("mysql", *dbString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("DROP DATABASE IF EXISTS " + dbName)
	if err != nil {
		log.Fatal(err)
	}
}
