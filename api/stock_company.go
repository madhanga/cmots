package api

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/angel-one/go-utils/log"
)

func SyncStockCompany(db *sql.DB) error {
	op := `SyncStockCompany`
	ctx := context.Background()
	sm_res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SchemeMaster")
	log.Info(ctx).Msgf("job started %s", op)

	if err != nil {
		log.Error(ctx).Err(err).Msg("CMOTS SchemeMaster API is down")
		return err
	}

	insert_query := `DELETE from stock_company where mf_schcode in
	(SELECT DISTINCT a.mf_schcode from stock_company a
	INNER JOIN stock_company_staging b
	on a.mf_schcode=b.mf_schcode and a.invdate <> b.invdate);
	
	insert into stock_company (co_code,mf_schcode,invdate,co_name,no_shares,perc_hold,isin)
	select a.co_code,a.mf_schcode,a.invdate,a.co_name,SUM(a.no_shares),SUM(a.perc_hold),a.isin 
	from stock_company_staging a 
	LEFT JOIN stock_company b
	on a.mf_schcode=b.mf_schcode and a.invdate = b.invdate
	where b.mf_schcode is null 
	group by a.co_code,a.mf_schcode,a.invdate,a.co_name,a.isin`

	trunc_query := `TRUNCATE TABLE stock_company_staging`
	db.Exec(trunc_query)

	if err != nil {
		log.Error(ctx).Err(err).Msg("Error truncating stock_company_staging table")
		return err
	}
	query := `INSERT INTO stock_company_staging (co_code,mf_schcode,invdate,co_name,no_shares,perc_hold,isin) VALUES ($1,$2,$3,$4,$5,$6,$7)`

	schememaster_data := sm_res["data"].([]any)

	for _, stockcompany := range schememaster_data {
		stockcompany := stockcompany.(map[string]interface{})
		var x interface{} = stockcompany["mf_schcode"]
		mf_schcode := fmt.Sprintf("%v", x)
		url := "http://angelbrokingapi.cmots.com/api/PortfolioDetailsMonthly/" + mf_schcode
		pf_res, err := GetCMOTS(url)
		if err != nil {
			log.Error(ctx).Err(err).Msg("CMOTS PortfolioDetailsMonthly API is down")
			return err
		}
		// Below condition is written if the api returns null data
		if pf_res["data"] != nil {

			portfolio_data := pf_res["data"].([]any)
			for _, stockcompany := range portfolio_data {
				portfolio_data := stockcompany.(map[string]interface{})
				co_code := portfolio_data["CO_CODE"]
				mf_schcode := portfolio_data["MF_SCHCODE"]
				co_name := portfolio_data["CO_NAME"]
				perc_hold := portfolio_data["PERC_HOLD"]
				no_shares := portfolio_data["NO_SHARES"]
				invdate := portfolio_data["INVDATE"]
				isin := portfolio_data["isin"]
				_, err := db.Exec(query, co_code, mf_schcode, invdate, co_name, no_shares, perc_hold, isin) //, mf_cocode
				if err != nil {
					log.Error(ctx).Err(err).Msg("Error inserting records in stock_company_staging table")
					return err
				}
			}

		} else {
			//mf_schcode_missed = mf_schcode_missed + mf_schcode
			println(mf_schcode)
		}

	}
	db.Exec(insert_query)

	log.Info(ctx).Msgf("job ended %s", op)
	return nil
}
