package api

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/angel-one/go-utils/log"
	"github.com/madhanga/cmots/constants"
)

func SyncBM(db *sql.DB) error {
	op := `SyncBenchmark`
	ctx := context.Background()
	log.Info(ctx).Msgf("job started %s", op)

	fh_res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/Fund_House")
	if err != nil {
		log.Error(ctx).Err(err).Msg("CMOTS Fund_House API is down")
		return err
	}
	trunc_query := `TRUNCATE TABLE Benchmark_staging`
	db.Exec(trunc_query)
	if err != nil {
		log.Error(ctx).Err(err).Msg("Error truncating Benchmark_staging table")
		return err
	}

	fh_data := fh_res["data"].([]any)
	for _, fundHouse := range fh_data {
		fundHouse := fundHouse.(map[string]interface{})
		mf_cocode := fundHouse["MF_COCODE"] //.(string) + fmt.Sprintf("%v", mf_cocode)

		res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SchemeReturns/-/" + fmt.Sprintf("%v", mf_cocode) + "/-/-/-/-/-/-")
		if err != nil {
			log.Error(ctx).Err(err).Msg("CMOTS SchemeReturns API is down")
			return err
		}
		//If the data for that amc code is not available in the api then continue the loop.
		if fmt.Sprintf("%v", res["data"]) == "<nil>" {
			//println(fmt.Sprintf("%v", mf_cocode))
			continue
		}

		data := res["data"].([]any)
		if err != nil {

			continue
		}

		val := ""
		for _, benchmark := range data {
			benchmark := benchmark.(map[string]interface{})
			MF_SCHCODE := benchmark["MF_SCHCODE"]
			isin := benchmark["isin"]
			if isin == nil {
				isin = ""
			}

			isin_Reinvestment := benchmark["isin_Reinvestment"]
			if isin_Reinvestment == nil {
				isin_Reinvestment = ""
			}

			BenchmarkName := benchmark["BenchmarkName"]
			if BenchmarkName == nil {
				BenchmarkName = ""
			}

			BM_1WRet := benchmark["BM_1WRet"]
			if BM_1WRet == nil {
				BM_1WRet = 0.0
			}

			BM_1MRet := benchmark["BM_1MRet"]
			if BM_1MRet == nil {
				BM_1MRet = 0.0
			}

			BM_3MRet := benchmark["BM_3MRet"]
			if BM_3MRet == nil {
				BM_3MRet = 0.0
			}

			BM_6MRet := benchmark["BM_6MRet"]
			if BM_6MRet == nil {
				BM_6MRet = 0.0
			}

			BM_1YRet := benchmark["BM_1YRet"]
			if BM_1YRet == nil {
				BM_1YRet = 0.0
			}

			BM_3YRet := benchmark["BM_3YRet"]
			if BM_3YRet == nil {
				BM_3YRet = 0.0
			}

			BM_5YRet := benchmark["BM_5YRet"]
			if BM_5YRet == nil {
				BM_5YRet = 0.0
			}

			if val != "" {
				val = val + ","
			}
			val = val + fmt.Sprintf("(%f, '%s', '%s', %f, %f, %f, %f, %f, %f, %f, '%s', %f)", MF_SCHCODE, isin, isin_Reinvestment, BM_1WRet, BM_1MRet, BM_3MRet, BM_6MRet, BM_1YRet, BM_3YRet, BM_5YRet, BenchmarkName, mf_cocode)

			if len(fmt.Sprintf("%v", isin_Reinvestment)) == 12 {
				val = val + fmt.Sprintf(",(%f, '%s', %s, %f, %f, %f, %f, %f, %f, %f, '%s',%f)", MF_SCHCODE, isin_Reinvestment, " ' ' ", BM_1WRet, BM_1MRet, BM_3MRet, BM_6MRet, BM_1YRet, BM_3YRet, BM_5YRet, BenchmarkName, mf_cocode)
			}

		}
		query := `INSERT INTO Benchmark_staging(mf_schcode,isin,isin_reinvestment,bm_1week_return,bm_1month_return,bm_3month_return,bm_6month_return,bm_1year_return,bm_3year_return,bm_5year_return,benchmarkname,mf_cocode) VALUES ` + val

		_, err = db.Exec(query)
		if err != nil {

			return err
		}

	}
	db.Exec(constants.Update_Benchmark)
	log.Info(ctx).Msgf("job ended %s", op)
	return nil
}
