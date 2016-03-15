package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/Pallinder/go-randomdata"
	_ "github.com/go-sql-driver/mysql"

	. "github.com/onsi/gomega"
)

var dbString = flag.String("db", "", "sql connection string")

const (
	dbName         = "JenLikesCats"
	rowsToSeed     = 1000
	maxConnections = 10
)

type Cat struct {
	Name    string
	Species string
}

func failHandler(message string, callerSkip ...int) {
	log.Fatal(message)
}

func main() {
	RegisterFailHandler(failHandler)

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

	fmt.Println("running scenario 1")
	scenario1(db)
	wait()
	fmt.Println("running scenario 2")
	scenario2(db)
	wait()
	fmt.Println("running scenario 3")
	scenario3(db)
	wait()
}

func wait() {
	fmt.Print("hit ENTER when ready to continue")

	reader := bufio.NewReader(os.Stdin)
	reader.ReadString('\n')
}

func scenario1(db *sql.DB) {
	cats := read(db)
	dropLeader()
	finalCats := read(db)
	compare(cats, finalCats)
}

func scenario2(db *sql.DB) {
	initCats := read(db)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				intermediateCats := read(db)
				compare(initCats, intermediateCats)
			}
		}
	}()

	dropLeader()
	close(stop)
	finalCats := read(db)
	compare(initCats, finalCats)
}

func scenario3(db *sql.DB) {
	cats := read(db)
	stop := make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				cats = append(cats, insertCat(db))
			}
		}
	}()

	dropLeader()
	close(stop)
	wg.Wait()
	finalCats := read(db)
	compare(cats, finalCats)
}

func read(db *sql.DB) []Cat {
	cats := []Cat{}
	rows, err := db.Query("SELECT name, species FROM cats ORDER BY id")
	if err != nil {
		log.Fatal(err)
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

func compare(initalCats, finalCats []Cat) {
	Expect(finalCats).To(HaveLen(len(initalCats)))
}

func killACat(db *sql.DB) {
	db.Exec("DELETE FROM cats ORDER BY RAND() LIMIT 1")
}

func seedDatabase(db *sql.DB) {
	wg := sync.WaitGroup{}
	for i := 0; i < rowsToSeed; i++ {
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
		log.Fatal("failed to insert cat. meow.", err)
	}
	return cat
}

func createTable(db *sql.DB) {
	_, err := db.Exec("CREATE TABLE cats (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(20), species VARCHAR(20) )")
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
