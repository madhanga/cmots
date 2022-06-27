package api

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/angel-one/go-utils/log"
	"github.com/madhanga/cmots/constants"
)

func SyncStockCompany(db *sql.DB) error {
	op := `SyncStockCompany`
	ctx := context.Background()

	// bm_res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SchemeMaster")
	log.Info(ctx).Msgf("job started %s", op)

	// if err != nil {
	// 	log.Error(ctx).Err(err).Msg("CMOTS SchemeMaster API is down")
	// 	return err
	// }
	val := ""
	var Query string

	fh_res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SchemeMaster")
	if err != nil {
		log.Error(ctx).Err(err).Msg("CMOTS SchemeMaster API is down")
		return err
	}
	trunc_query := `TRUNCATE TABLE Stock_company_staging`
	db.Exec(trunc_query)
	cnt := 0
	sm_data := fh_res["data"].([]any)
	for _, schememaster := range sm_data {
		fundHouse := schememaster.(map[string]interface{})
		mf_schcode := fundHouse["mf_schcode"] //.(string) " + fmt.Sprintf("%v", mf_schcode) + "
		//println(fmt.Sprintf("%v", mf_schcode))
		res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/PortfolioDetailsMonthly/" + fmt.Sprintf("%v", mf_schcode))
		if err != nil {
			log.Error(ctx).Err(err).Msg("CMOTS PortfolioDetailsMonthly API is down")
			return err
		}
		if fmt.Sprintf("%v", res["data"]) == "<nil>" {
			//println(fmt.Sprintf("%v", mf_schcode))
			continue
		}
		data := res["data"].([]any)
		if err != nil {

			continue
		}

		for _, stockcompany := range data {
			stockcompany := stockcompany.(map[string]interface{})
			MF_SCHCODE := stockcompany["MF_SCHCODE"]
			CO_CODE := stockcompany["CO_CODE"]

			isin := stockcompany["isin"]
			if isin == nil {
				isin = ""
			}

			// isin_Reinvestment := stockcompany["isin_Reinvestment"]
			// if isin_Reinvestment == nil {
			// 	isin_Reinvestment = ""
			// }

			INVDATE := stockcompany["INVDATE"]
			if INVDATE == nil {
				INVDATE = ""
			}

			PERC_HOLD := stockcompany["PERC_HOLD"]
			if PERC_HOLD == nil {
				PERC_HOLD = 0.0
			}

			MKTVALUE := stockcompany["MKTVALUE"]
			if MKTVALUE == nil {
				MKTVALUE = 0.0
			}

			NO_SHARES := stockcompany["NO_SHARES"]
			if NO_SHARES == nil {
				NO_SHARES = 0.0
			}

			co_name := stockcompany["co_name"]
			if co_name == nil {
				co_name = ""
			}

			// Rating := stockcompany["Rating"]
			// if Rating == nil {
			// 	Rating = ""
			// }

			if val != "" {
				val = val + ","
			}
			val = val + fmt.Sprintf("('%s', %f, '%s', '%s', %f, %f, %f, '%s')", MF_SCHCODE, CO_CODE, isin, INVDATE, PERC_HOLD, MKTVALUE, NO_SHARES, co_name)
			// cnt = cnt + 1
			// println(cnt)
			// if cnt == 5 {
			// 	break
			// }
		}
		println(cnt, (fmt.Sprintf("%v", mf_schcode)))
		cnt = cnt + 1

		// if cnt == 5000 {
		// 	break
		// }
	}
	Query = `INSERT INTO stock_company_staging(MF_SCHCODE, CO_CODE, isin, INVDATE, PERC_HOLD, MKTVALUE, NO_SHARES, co_name) VALUES ` + val + `;`
	println(Query)
	_, err = db.Exec(Query)
	if err != nil {
		//println(query)
		return err
	}
	db.Exec(constants.Update_stockcompany)
	log.Info(ctx).Msgf("job ended %s", op)
	return nil
}
