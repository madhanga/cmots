package api

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/angel-one/go-utils/log"
	"github.com/madhanga/cmots/constants"
)

func SyncSchemeMaster(db *sql.DB) error {
	op := `SyncSchemePlan`
	ctx := context.Background()
	log.Info(ctx).Msgf("job started %s", op)

	sm_res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SchemeMaster")
	if err != nil {
		log.Error(ctx).Err(err).Msg("CMOTS SchemeMaster API is down")
		return err
	}
	sm_data := sm_res["data"].([]any)

	trunc_query := `TRUNCATE TABLE schememaster_staging`
	db.Exec(trunc_query)
	if err != nil {
		return err
	}

	val := ""

	for _, sm_schemeplan := range sm_data {
		sm_schemeplan := sm_schemeplan.(map[string]interface{})
		mf_schcode := sm_schemeplan["mf_schcode"]
		mf_cocode := sm_schemeplan["mf_cocode"]
		isin := sm_schemeplan["isin"]
		if isin == nil {
			isin = ""
		}
		scheme_name := sm_schemeplan["sch_name"]
		if sm_schemeplan["InvestmentType"] == "Dividend" {
			scheme_name = scheme_name.(string) + " - Payout"
		}
		exit_load_desc := sm_schemeplan["ExitLoad"]
		if exit_load_desc == nil {
			exit_load_desc = ""
		}
		exit_load_flag := sm_schemeplan["exit_load_flag"]
		if exit_load_flag == nil {
			exit_load_flag = ""
		}
		riskometer := "" //sm_schemeplan["navrs"]
		scheme_plan := sm_schemeplan["SchemeInvestmentType"]
		if scheme_plan == nil {
			scheme_plan = ""
		}
		benchmark_index := sm_schemeplan["BenchmarkName"]
		if benchmark_index == nil {
			benchmark_index = ""
		}
		riskometervalue := sm_schemeplan["riskometervalue"]
		if riskometervalue == nil {
			riskometervalue = ""
		}
		sip_min_amount := sm_schemeplan["MinInvestment_SIP"]
		if sip_min_amount == nil {
			sip_min_amount = 0.0
		}
		sip_frequency := sm_schemeplan["frequency"]
		if sip_frequency == nil {
			sip_frequency = ""
		}
		dividend_reinvestment_flag := sm_schemeplan["dividend_reinvestment_flag"]
		InvestmentType := sm_schemeplan["InvestmentType"]

		isin_Reinvestment := sm_schemeplan["isin_Reinvestment"]
		if isin_Reinvestment == nil {
			isin_Reinvestment = ""
		}
		purchase_allowed := sm_schemeplan["purchase_allowed"]
		if purchase_allowed == nil {
			purchase_allowed = ""
		}
		sip_flag := sm_schemeplan["sip_flag"]

		maximum_purchase_amount := sm_schemeplan["maximum_purchase_amount"]
		if maximum_purchase_amount == nil {
			maximum_purchase_amount = 0.0
		}
		expense_ratio := sm_schemeplan["expense_ratio"]
		if expense_ratio == nil {
			expense_ratio = 0.0
		}

		lock_in_period := sm_schemeplan["lock_in_period"]
		if lock_in_period == nil {
			lock_in_period = 0.0
		}
		lock_in_period_flag := sm_schemeplan["lock_in_period_flag"]
		if lock_in_period == nil {
			lock_in_period = ""
		}
		lumsum_max_amount := sm_schemeplan["lumsum_max_amount"]
		if lumsum_max_amount == nil {
			lumsum_max_amount = 0.0
		}

		FundManager_JoiningDate := sm_schemeplan["FundManager_JoiningDate"]
		if FundManager_JoiningDate == nil {
			FundManager_JoiningDate = "1900-01-01"
		}

		nav_date := sm_schemeplan["navdate"]
		if nav_date == nil {
			nav_date = "1900-01-01"
		}
		nav_value := sm_schemeplan["navrs"]
		if nav_value == nil {
			nav_value = 0.0
		}
		if val != "" {
			val = val + ","
		}
		val = val + fmt.Sprintf("(%f, %f, '%s','%s','%s', '%s','%s','%s','%s', %f, '%s','%s', '%s', '%s','%s','%s',%f,%f,%f,'%s', %f,'%s','%s','%s', %f)", mf_schcode, mf_cocode, isin, strings.ReplaceAll(fmt.Sprintf("%v", scheme_name), "'", "''"), exit_load_desc, riskometer, scheme_plan, benchmark_index, riskometervalue, sip_min_amount, sip_frequency, dividend_reinvestment_flag, InvestmentType, isin_Reinvestment, purchase_allowed, sip_flag, maximum_purchase_amount, expense_ratio, lock_in_period, lock_in_period_flag, lumsum_max_amount, FundManager_JoiningDate, exit_load_flag, nav_date, nav_value)

		if InvestmentType == `Dividend` {
			scheme_name = sm_schemeplan["sch_name"]
			scheme_name := scheme_name.(string) + " - Reinvestment"
			dividend_reinvestment_flag := "Z"

			val = val + fmt.Sprintf(",(%f, %f, '%s','%s','%s', '%s','%s','%s','%s', %f, '%s','%s', '%s', '%s','%s','%s',%f,%f,%f,'%s', %f,'%s','%s','%s', %f)", mf_schcode, mf_cocode, isin_Reinvestment, strings.ReplaceAll(fmt.Sprintf("%v", scheme_name), "'", "''"), exit_load_desc, riskometer, scheme_plan, benchmark_index, riskometervalue, sip_min_amount, sip_frequency, dividend_reinvestment_flag, InvestmentType, isin_Reinvestment, purchase_allowed, sip_flag, maximum_purchase_amount, expense_ratio, lock_in_period, lock_in_period_flag, lumsum_max_amount, FundManager_JoiningDate, exit_load_flag, nav_date, nav_value)

		}

	}
	query := `INSERT INTO schememaster_staging(mf_schcode, mf_cocode, isin, scheme_name, exit_load_desc, riskometer, scheme_plan, benchmark_index, riskometervalue, sip_min_amount, sip_frequency, dividend_reinvestment_flag, InvestmentType, isin_Reinvestment, purchase_allowed, sip_flag, maximum_purchase_amount, expense_ratio, lock_in_period, lock_in_period_flag, lumsum_max_amount, FundManager_JoiningDate,exit_load_flag,nav_date,nav_value) VALUES ` + val

	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////

	nav_res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SchemeNAVdetails/all/all/all/all")
	if err != nil {
		return err
	}
	nav_data := nav_res["data"].([]any)

	trunc_query = `TRUNCATE TABLE schemenavdetails_staging`
	db.Exec(trunc_query)
	if err != nil {
		log.Error(ctx).Err(err).Msg("Error truncating sm_scheme_plan_staging table")
		return err
	}

	val = ""

	for _, sm_schemeplan := range nav_data {
		sm_schemeplan := sm_schemeplan.(map[string]interface{})

		mf_schcode := sm_schemeplan["MF_SCHCODE"]
		isin := sm_schemeplan["isin"]
		if isin == nil {
			isin = ""
		}

		isin_Reinvestment := sm_schemeplan["isin_Reinvestment"]
		if isin_Reinvestment == nil {
			isin_Reinvestment = ""
		}

		nav_date := sm_schemeplan["navdate"]
		if nav_date == nil {
			nav_date = "1900-01-01"
		}

		nav_value := sm_schemeplan["navrs"]
		if nav_value == nil {
			nav_value = 0.0
		}
		entry_load := sm_schemeplan["entry"]
		if entry_load == nil {
			entry_load = ""
		}

		launch_date := sm_schemeplan["launc_date"]
		if launch_date == nil {
			launch_date = "1900-01-01"
		}

		one_month_return := sm_schemeplan["1month"]
		if one_month_return == nil {
			one_month_return = 0.0
		}
		three_month_return := sm_schemeplan["3month"]
		if three_month_return == nil {
			three_month_return = 0.0
		}
		six_month_return := sm_schemeplan["6month"]
		if six_month_return == nil {
			six_month_return = 0.0
		}
		one_year_return := sm_schemeplan["1year"]
		if one_year_return == nil {
			one_year_return = 0.0
		}
		three_year_return := sm_schemeplan["3year"]
		if three_year_return == nil {
			three_year_return = 0.0
		}
		five_year_return := sm_schemeplan["5year"]
		if five_year_return == nil {
			five_year_return = 0.0
		}
		inception_return := sm_schemeplan["inception"]
		if inception_return == nil {
			inception_return = 0.0
		}

		asset_size := sm_schemeplan["size"]
		if asset_size == nil {
			asset_size = 0.0
		}

		Category := sm_schemeplan["Category"]
		if Category == nil {
			Category = ""
		}
		SubCategory := sm_schemeplan["SubCategory"]
		if SubCategory == nil {
			SubCategory = ""
		}

		if val != "" {
			val = val + ","
		}
		val = val + fmt.Sprintf("(%f,'%s','%s','%s',%f,'%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,'%s','%s')", mf_schcode, isin, isin_Reinvestment, nav_date, nav_value, entry_load, launch_date, asset_size, one_year_return, five_year_return, inception_return, three_year_return, one_month_return, three_month_return, six_month_return, Category, SubCategory)

	}
	query = `INSERT INTO schemenavdetails_staging(mf_schcode,isin,isin_reinvestment,nav_date,nav_value,entry_load,launch_date,asset_size,one_year_return,five_year_return,inception_return,three_year_return,one_month_return,three_month_return,six_month_return,category,sub_category) VALUES ` + val

	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	db.Exec(constants.Insert_update_schemeplan)
	db.Exec(constants.Update_navdetails)
	db.Exec(constants.Update_scheme_master)
	log.Info(ctx).Msgf("job ended %s", op)
	return nil
}
