package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/madhanga/cmots/api"
)

func main() {

	db := db()
	defer db.Close()

	err := api.SyncFundHouses(db)
	if err != nil {
		log.Fatal(err)
	}

	err = api.SyncSchemeMaster(db)
	if err != nil {
		log.Fatal(err)
	}

	err = api.SyncSipDates(db)
	if err != nil {
		log.Fatal(err)
	}

	err = api.SyncBM(db)
	if err != nil {
		log.Fatal(err)
	}

}

func db() *sql.DB {

	dbUserName := "mfcore"
	dbPassword := "mfcore123"
	dbName := "mfcore"
	dbHost := "mf-core-uat.c0iswfmnnzar.ap-south-1.rds.amazonaws.com"

	// dbUserName := os.Getenv("DATABASE_USERNAME")
	// dbPassword := os.Getenv("DATABASE_PASSWORD")
	// dbName := os.Getenv("DATABASE_NAME")
	// dbHost := os.Getenv("DATABASE_URL")

	log.Println(dbUserName, dbPassword, dbName, dbHost)
	if dbUserName == "" || dbPassword == "" || dbName == "" || dbHost == "" {
		log.Fatal("Database credentials not set")
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=disable&connect_timeout=10", dbUserName, dbPassword, dbHost, dbName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	return db
}
