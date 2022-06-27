package api

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/angel-one/go-utils/log"
)

func SyncSipDates(db *sql.DB) error {
	op := `SyncSipDates`
	ctx := context.Background()
	log.Info(ctx).Msgf("job started %s", op)

	res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SIP_Dates/SIP")
	if err != nil {
		log.Error(ctx).Err(err).Msg("CMOTS SIP_Dates API is down")
		return err
	}

	trunc_query := `TRUNCATE TABLE sipdates_staging`
	db.Exec(trunc_query)

	data := res["data"].([]any)

	val := ""
	for _, sip_dates := range data {
		sip_dates := sip_dates.(map[string]interface{})
		MF_SCHCODE := sip_dates["MF_SCHCODE"]
		MF_COCODE := sip_dates["MF_COCODE"]

		PLAN := sip_dates["PLAN"]
		if PLAN == nil {
			PLAN = "null"
		}

		Frequency := sip_dates["Frequency"]
		if Frequency == nil {
			Frequency = "null"
		}

		D1 := sip_dates["D1"]
		if D1 == nil {
			D1 = 0.0
		}

		D2 := sip_dates["D2"]
		if D2 == nil {
			D2 = 0.0
		}

		D3 := sip_dates["D3"]
		if D3 == nil {
			D3 = 0.0
		}

		D4 := sip_dates["D4"]
		if D4 == nil {
			D4 = 0.0
		}

		D5 := sip_dates["D5"]
		if D5 == nil {
			D5 = 0.0
		}

		D6 := sip_dates["D6"]
		if D6 == nil {
			D6 = 0.0
		}

		D7 := sip_dates["D7"]
		if D7 == nil {
			D7 = 0.0
		}

		D8 := sip_dates["D8"]
		if D8 == nil {
			D8 = 0.0
		}

		D9 := sip_dates["D9"]
		if D9 == nil {
			D9 = 0.0
		}

		D10 := sip_dates["D10"]
		if D10 == nil {
			D10 = 0.0
		}

		ANY := sip_dates["ANY"]
		if ANY == nil {
			ANY = "null"
		}

		isin := sip_dates["isin"]
		if isin == nil {
			isin = "null"
		}

		isin_Reinvestment := sip_dates["isin_Reinvestment"]
		if isin_Reinvestment == nil {
			isin_Reinvestment = "null"
		}

		if val != "" {
			val = val + ","
		}
		val = val + fmt.Sprintf("(%f, %f, '%s','%s',%f, %f, %f, %f, %f, %f, %f, %f, %f, %f,'%s','%s','%s')", MF_SCHCODE, MF_COCODE, PLAN, Frequency, D1, D2, D3, D4, D5, D6, D7, D8, D9, D10, ANY, isin, isin_Reinvestment)

	}
	query := `INSERT INTO sipdates_staging(MF_SCHCODE, MF_COCODE, PLAN,Frequency,D1,D2,D3,D4,D5,D6,D7,D8,D9,D10,any_day,isin,isin_Reinvestment) VALUES ` + val
	_, err = db.Exec(query)
	if err != nil {

		println(query)
		return err
	}
	log.Info(ctx).Msgf("job ended %s", op)
	return nil

}
