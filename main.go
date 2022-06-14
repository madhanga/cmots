package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	db := db()
	defer db.Close()

	err := syncFundHouses(db)
	if err != nil {
		log.Fatal(err)
	}

}

func db() *sql.DB {
	dbUserName := os.Getenv("DATABASE_USERNAME")
	dbPassword := os.Getenv("DATABASE_PASSWORD")
	dbName := os.Getenv("DATABASE_NAME")
	dbHost := os.Getenv("DATABASE_URL")
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

func syncFundHouses(db *sql.DB) error {
	res, err := getCMOTS("http://angelbrokingapi.cmots.com/api/Fund_House")
	if err != nil {
		return err
	}

	query := `INSERT INTO fund_house (name) VALUES ($1)`
	data := res["data"].([]any)
	for _, fundHouse := range data {
		fundHouse := fundHouse.(map[string]interface{})
		name := fundHouse["nameamc"].(string)
		_, err := db.Exec(query, name)
		if err != nil {
			log.Printf("Failed to add fund house for %s: %v", name, err)
			continue
		}
		fmt.Println("added found house ", name)
	}

	return nil
}

func getCMOTS(url string) (map[string]any, error) {
	bearerToken := "Bearer FkLpiVedKizrjkML771_wJ-vEKMPKVKrNzZHSAe2yipPt8jDyssu-l8GOVh1UrZs8dI05kNT_Jyjf7-Hi9Q7QDLaod844f_wb31hxDtBpWcf3DekV1AsIGifKUJJePgRw8BzC-xg-7Vb0ylK8YbgY72JYYPNFp-Vqs6xqA0W0wsGo9ouu2CXf5MPHW7qLrMdpQjLGp6EZJIKVGNloAvjfnhKoajHqVoUiAUbpZJfM-o6epe-edbRRN5WxN2FuIVPoEA9v-Uh_LIK5k5p9wm5xx5cww72r1uc3SD3TSo2nosdhreIFCcyMxLGNzG-In0f"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", bearerToken)

	client := http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var data map[string]any
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func getLogo() map[string][2]string {
	return map[string][2]string{
		"6051.0": {"https://d3usff6y6s0r8b.cloudfront.net/sbi_angel.svg", "https://www.sbimf.com/en-us/offer-document-sid-kim"},
	}
}
