package constants

const (
	Insert_update_schemeplan = `insert into scheme_plan(mf_schcode,isin,scheme_name,dividend_reinvestment_flag,sip_flag,exit_load_flag,exit_load_desc,lock_in_period_flag,
	lock_in_period,nav_value,nav_date,expense_ratio,riskometer,scheme_plan,benchmark_index,riskometervalue,sip_frequency,mf_cocode,is_active,sip_min_amount)
	select distinct ss.mf_schcode ,ss.isin ,ss.scheme_name ,ss.dividend_reinvestment_flag ,ss.sip_flag ,ss.exit_load_flag,ss.exit_load_desc,ss.lock_in_period_flag,  
	ss.lock_in_period ,ss.nav_value,ss.nav_date,ss.expense_ratio ,
	case when ss.riskometervalue='Low' then 0 when ss.riskometervalue='Moderately Low' then 1
	when ss.riskometervalue='Moderate' then 2 when ss.riskometervalue='Moderately High' then 3 when ss.riskometervalue='High' then 4
	when ss.riskometervalue='Very High' then 5  else 6 end as riskometervalue, 
	case when ss.scheme_plan = 'Direct Fund' then 'DIRECT' else 'REGULAR' end as scheme_plan,
	ss.benchmark_index ,ss.riskometervalue ,ss.sip_frequency ,ss.mf_cocode,1,ss.sip_min_amount 
	from schememaster_staging ss 
	left join scheme_plan sp 
	on ss.isin =sp.isin 
	where sp.isin is null and ss.isin <> '';
	
	update scheme_plan
	set mf_schcode=ss.mf_schcode,scheme_name=ss.scheme_name,dividend_reinvestment_flag=ss.dividend_reinvestment_flag ,sip_flag=ss.sip_flag,
	exit_load_flag=ss.exit_load_flag ,exit_load_desc=ss.exit_load_desc ,lock_in_period_flag=ss.lock_in_period_flag ,  
	lock_in_period=ss.lock_in_period  ,nav_value=ss.nav_value ,nav_date=ss.nav_date ,expense_ratio=ss.expense_ratio,
	riskometer  = case when ss.riskometervalue='Low' then 0 when ss.riskometervalue='Moderately Low' then 1
	when ss.riskometervalue='Moderate' then 2 when ss.riskometervalue='Moderately High' then 3 when ss.riskometervalue='High' then 4
	when ss.riskometervalue='Very High' then 5  else 6 end , 
	scheme_plan = case when ss.scheme_plan = 'Direct Fund' then 'DIRECT' else 'REGULAR' end,
	benchmark_index = ss.benchmark_index  ,riskometervalue=ss.riskometervalue,sip_frequency=ss.sip_frequency ,mf_cocode=ss.mf_cocode,is_active =1,
	sip_min_amount=ss.sip_min_amount  
	from schememaster_staging ss 
	--left join scheme_plan sp 
	where scheme_plan.isin =ss.isin;`

	Update_navdetails = `update scheme_plan 
	set asset_size=b.asset_size,
	launch_date=b.launch_date,
	one_year_return=b.one_year_return,
	five_year_return = b.five_year_return,
	inception_return=b.inception_return,
	three_year_return=b.three_year_return,
	one_month_return=b.one_month_return,
	three_month_return=b.three_month_return,
	six_month_return=b.six_month_return,
	category=b.category,
	sub_category=b.sub_category,
	is_active = 1
	from schemenavdetails_staging b
	where scheme_plan.mf_schcode = b.mf_schcode;`

	Update_scheme_master = `update scheme_plan
	set scheme_code=sm.scheme_code,
	purchase_txn_mode=sm.purchase_txn_mode,
	redemption_txn_mode=sm.redemption_txn_mode,
	stp_flag=sm.stp_flag,
	swp_flag=sm.swp_flag,
	switch_flag=sm.switch_flag,
	settlement_type=sm.settlement_type,
	prev_nav_value=sm.prev_nav_value,
	prev_nav_date=sm.prev_nav_date,
	minimum_pur_amt  = sm.minimum_pur_amt,
	maximum_pur_amt = sm.maximum_pur_amt,
	minimum_redemption_qty = sm.minimum_redemption_qty,
	purchase_allowed = sm.purchase_allowed ,
	redemption_allowed = sm.redemption_allowed,
	sip_max_amount = 10000000,
	sip_min_installment_no =1,
	sip_max_installment_no =9999,
	sip_allowed_days='1,5,7,15,16,14,25,28,10'
	from scheme_master sm 
	where scheme_plan.isin=sm.isin
	and  sm.scheme_code not like '%-L1%'
	and  sm.scheme_code not like '%-I%' 
	and  sm.scheme_code not like '%-L0%';`

	Update_ratings = `update scheme_plan 
	set 
	rating_crisil = b.crisil_rating ,
	rating_morning_star = b.morningstar_rating ,
	rating_value_research = b.value_research_rating ,
	arq_rating = b.arq_rating 
	from arq_score  b 
	where scheme_plan.isin = b.isin;`

	Update_Benchmark = `update scheme_plan
	set
	bm_1week_return=sm.bm_1week_return ,
	bm_1month_return=sm.bm_1month_return,
	bm_3month_return=sm.bm_3month_return,
	bm_6month_return=sm.bm_6month_return,
	bm_1year_return=sm.bm_1year_return,
	bm_3year_return=sm.bm_3year_return,
	bm_5year_return=sm.bm_5year_return 
	from Benchmark_staging sm 
	where scheme_plan.isin=sm.isin;
	
	update scheme_plan 
	set bm_1week_return=null where bm_1week_return=0;
	update scheme_plan 
	set bm_1month_return=null where bm_1month_return=0;
	update scheme_plan 
	set bm_3month_return=null where bm_3month_return=0;
	update scheme_plan 
	set bm_6month_return=null where bm_6month_return=0;
	update scheme_plan 
	set bm_1year_return=null where bm_1year_return=0;
	update scheme_plan 
	set bm_3year_return=null where bm_3year_return=0;
	update scheme_plan 
	set bm_5year_return=null where bm_5year_return=0; 
	delete from scheme_plan where scheme_code is null;`

	Update_stockcompany = `
	truncate table stock_company;
	INSERT INTO stock_company(MF_SCHCODE, CO_CODE, isin, INVDATE, PERC_HOLD, MKTVALUE, NO_SHARES, co_name) 
	select MF_SCHCODE, CO_CODE, isin, INVDATE, PERC_HOLD, MKTVALUE, NO_SHARES, co_name from stock_company;`
)
