package api

import (
	"context"
	"database/sql"

	"github.com/angel-one/go-utils/log"
)

func SyncSchemePlan(db *sql.DB) error {
	op := `SyncSchemePlan`
	ctx := context.Background()
	log.Info(ctx).Msgf("job started %s", op)

	insert_update_query :=
		`insert into scheme_plan(scheme_name,nav_value,nav_date,asset_size,launch_date,one_year_return,five_year_return,inception_return,
		three_year_return,one_month_return,three_month_return,six_month_return,category,sub_category,mf_schcode,isin)
		select a.scheme_name,a.nav_value,a.nav_date,a.asset_size,a.launch_date,a.one_year_return,a.five_year_return,a.inception_return,
		a.three_year_return,a.one_month_return,a.three_month_return,a.six_month_return,a.category,a.sub_category,a.mf_schcode,a.isin 
		from scheme_plan_staging a LEFT join scheme_plan b on a.mf_schcode = b.mf_schcode where b.mf_schcode is null;
	
		update scheme_plan a set mf_cocode=b.mf_cocode,isin=b.isin,scheme_name=b.scheme_name,exit_load_desc=b.exit_load_desc,
        riskometer=b.riskometer,scheme_plan=b.scheme_plan,benchmark_index=b.benchmark_index,
        riskometervalue=b.riskometervalue,sip_min_amount=b.sip_min_amount,sip_frequency=b.sip_frequency,
        dividend_reinvestment_flag = b.dividend_reinvestment_flag,sip_flag = b.sip_flag,expense_ratio = b.expense_ratio,
        lock_in_period = b.lock_in_period,lock_in_period_flag = b.lock_in_period_flag,lumsum_max_amount = b.maximum_purchase_amount,
        updated_date_time = NOW()
        from sm_scheme_plan_staging b 
        where a.mf_schcode = b.mf_schcode`

	res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SchemeNAVdetails/all/all/all/all")
	if err != nil {
		log.Error(ctx).Err(err).Msg("CMOTS SchemeMaster API is down")
		return err
	}
	data := res["data"].([]any)

	trunc_query := `TRUNCATE TABLE scheme_plan_staging`
	db.Exec(trunc_query)
	if err != nil {
		log.Error(ctx).Err(err).Msg("Error truncating scheme_plan_staging table")
		return err
	}
	query := `INSERT INTO scheme_plan_staging(scheme_name,nav_value,nav_date,asset_size,launch_date,one_year_return,five_year_return,inception_return,
			 three_year_return,one_month_return,three_month_return,six_month_return,category,sub_category,mf_schcode,isin)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`

	// update_query := `update scheme_plan set scheme_name=b.scheme_name,nav_value=b.nav_value,nav_date=b.nav_date,
	// asset_size=b.asset_size,launch_date=b.launch_date,one_year_return=b.one_year_return,five_year_return=b.five_year_return,
	// inception_return=b.inception_return,three_year_return=b.three_year_return,one_month_return=b.one_month_return,three_month_return=b.three_month_return,
	// six_month_return=b.six_month_return,category=b.category,sub_category=b.sub_category,isin=b.isin
	// from scheme_plan_staging b
	// where scheme_plan.mf_schcode=b.mf_schcode`

	for _, schemeplan := range data {
		schemeplan := schemeplan.(map[string]interface{})
		scheme_name := schemeplan["Schemename"]
		nav_value := schemeplan["navrs"]
		nav_date := schemeplan["navdate"]
		asset_size := schemeplan["size"]
		launch_date := schemeplan["launc_date"]
		one_year_return := schemeplan["1year"]
		three_year_return := schemeplan["3year"]
		five_year_return := schemeplan["5year"]
		one_month_return := schemeplan["1month"]
		three_month_return := schemeplan["3month"]
		six_month_return := schemeplan["6month"]
		inception_return := schemeplan["inception"]
		category := schemeplan["Category"]
		sub_category := schemeplan["SubCategory"]
		mf_schcode := schemeplan["MF_SCHCODE"]
		isin := schemeplan["isin"]

		_, err := db.Exec(query, scheme_name, nav_value, nav_date, asset_size, launch_date, one_year_return, five_year_return, inception_return,
			three_year_return, one_month_return, three_month_return, six_month_return, category, sub_category, mf_schcode, isin)
		if err != nil {
			//log.Printf("Failed to add fund house for %s: %v", name, err)
			continue
		}
		//fmt.Println("added found house ", name)
	}

	sm_res, err := GetCMOTS("http://angelbrokingapi.cmots.com/api/SchemeMaster")
	if err != nil {
		return err
	}
	sm_data := sm_res["data"].([]any)

	trunc_query = `TRUNCATE TABLE sm_scheme_plan_staging`
	db.Exec(trunc_query)
	if err != nil {
		log.Error(ctx).Err(err).Msg("Error truncating sm_scheme_plan_staging table")
		return err
	}
	sm_query := `INSERT INTO sm_scheme_plan_staging(mf_schcode,mf_cocode,isin,scheme_name,exit_load_desc,
		riskometer,scheme_plan,benchmark_index
		,riskometervalue,sip_min_amount,sip_frequency,
		dividend_reinvestment_flag,InvestmentType,isin_Reinvestment,purchase_allowed,sip_flag,maximum_purchase_amount,
		expense_ratio, lock_in_period, lock_in_period_flag, lumsum_max_amount)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`

	for _, sm_schemeplan := range sm_data {
		sm_schemeplan := sm_schemeplan.(map[string]interface{})
		mf_schcode := sm_schemeplan["mf_schcode"]
		mf_cocode := sm_schemeplan["mf_cocode"]
		isin := sm_schemeplan["isin"]
		scheme_name := sm_schemeplan["sch_name"]
		exit_load_desc := sm_schemeplan["ExitLoad"]
		riskometer := "" //sm_schemeplan["navrs"]
		scheme_plan := sm_schemeplan["SchemeInvestmentType"]
		benchmark_index := sm_schemeplan["BenchmarkName"]
		riskometervalue := sm_schemeplan["riskometervalue"]
		sip_min_amount := sm_schemeplan["MinInvestment_SIP"]
		sip_frequency := sm_schemeplan["frequency"]
		dividend_reinvestment_flag := sm_schemeplan["dividend_reinvestment_flag"]
		InvestmentType := sm_schemeplan["InvestmentType"]
		isin_Reinvestment := sm_schemeplan["isin_Reinvestment"]
		purchase_allowed := sm_schemeplan["purchase_allowed"]
		sip_flag := sm_schemeplan["sip_flag"]
		maximum_purchase_amount := sm_schemeplan["maximum_purchase_amount"]
		expense_ratio := sm_schemeplan["expense_ratio"]
		lock_in_period := sm_schemeplan["lock_in_period"]
		lock_in_period_flag := sm_schemeplan["lock_in_period_flag"]
		lumsum_max_amount := sm_schemeplan["lumsum_max_amount"]

		_, err := db.Exec(sm_query, mf_schcode, mf_cocode, isin, scheme_name, exit_load_desc,
			riskometer, scheme_plan, benchmark_index, riskometervalue, sip_min_amount, sip_frequency,
			dividend_reinvestment_flag, InvestmentType, isin_Reinvestment, purchase_allowed, sip_flag, maximum_purchase_amount,
			expense_ratio, lock_in_period, lock_in_period_flag, lumsum_max_amount)

		if err != nil {
			//log.Printf("Failed to add fund house for %s: %v", name, err)
			continue
		}

		if InvestmentType == `Dividend` && isin_Reinvestment != nil {
			scheme_name := scheme_name.(string) + " - Reinvestment"
			dividend_reinvestment_flag := "Z"

			_, err := db.Exec(sm_query, mf_schcode, mf_cocode, isin_Reinvestment, scheme_name, exit_load_desc,
				riskometer, scheme_plan, benchmark_index, riskometervalue, sip_min_amount, sip_frequency,
				dividend_reinvestment_flag, InvestmentType, isin_Reinvestment, purchase_allowed, sip_flag, maximum_purchase_amount)

			if err != nil {
				//log.Printf("Failed to add fund house for %s: %v", name, err)
				continue
			}

		}
	}
	db.Exec(insert_update_query)
	log.Info(ctx).Msgf("job ended %s", op)
	return nil
}
