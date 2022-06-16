package api

import (
	"database/sql"
	"fmt"
	"log"
)

func SyncFundHouses(db *sql.DB) error {
	res, err := getCMOTS("http://angelbrokingapi.cmots.com/api/Fund_House")
	if err != nil {
		return err
	}

	trunc_query := `truncate table fund_house`
	db.Exec(trunc_query)

	query := `INSERT INTO fund_house (name,url_logo,amc_info_url,mf_cocode) VALUES ($1,$2,$3,$4)` //,$4
	data := res["data"].([]any)
	for _, fundHouse := range data {

		fundHouse := fundHouse.(map[string]interface{})
		name := fundHouse["nameamc"].(string)
		mf_cocode := fundHouse["MF_COCODE"]
		url_logo, amc_info_url, err1 := getLogo(fundHouse["MF_COCODE"].(float64))
		if err1 != "" {
			continue
			//log.Printf("Failed to add fund house logo for %s: %v", name, err1)
			//to be send to alert
		}
		_, err := db.Exec(query, name, url_logo, amc_info_url, mf_cocode) //, mf_cocode
		if err != nil {
			log.Printf("Failed to add fund house for %s: %v", name, err)
			continue
		}
		fmt.Println("added found house ", name)
	}

	return nil
}

func getLogo(cocode float64) (string, string, string) {
	logo_map := make(map[float64]*[2]string)
	logo_map[6051.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/sbi_angel.svg", "https://www.sbimf.com/en-us/offer-document-sid-kim"}
	logo_map[6946.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/canara_robeca_angel.svg", "https://www.canararobeco.com/forms-downloads/forms-and-information-documents/information-document/sid"}
	logo_map[37927.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/navi_angel.svg", "https://www.navimutualfund.com/downloads/Sid"}
	logo_map[22962.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/hsbc_global_angel.svg", "https://www.assetmanagement.hsbc.co.in/en/mutual-funds/fund-centre#openTab=0"}
	logo_map[20460.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/l&t_financial_angel.svg", "https://www.ltfs.com/companies/lnt-investment-management/downloads.html"}
	logo_map[21271.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/idfc_angel.svg", "https://idfcmf.com/download-centre/sid"}
	logo_map[28628.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/mirae_angel.svg", "https://www.miraeassetmf.co.in/downloads/forms"}
	logo_map[5946.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/aditya_birla_angel.svg", "https://mutualfund.adityabirlacapital.com/forms-and-downloads/forms"}
	logo_map[41332.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/indiabulls_angel.svg", "https://www.indiabullsamc.com/downloads/sid-sai/"}
	logo_map[40163.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/pgim_angel.svg", "https://www.pgimindiamf.com/forms-and-updates/sid-and-sai"}
	logo_map[38442.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/motilal_angel.svg", "https://www.motilaloswalmf.com/downloads/mutual-fund/SID"}
	logo_map[68409.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/mahindra_manulife_angel.svg", "https://www.licmf.com/sid-kim-sai"}
	logo_map[39921.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/idbi_angel.svg", "https://www.idbimutual.co.in/Downloads/SID#"}
	logo_map[6502.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/bnp_paribas_angel.svg", "https://www.barodabnpparibasmf.in/downloads"}
	//logo_map[6502.0] ={"NA","https://www.barodabnpparibasmf.in/downloads"}
	logo_map[29865.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/boi_angel.svg", "https://www.boiaxamf.in/investor-corner#t7"}
	logo_map[27488.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/invesco_angel.svg", "https://invescomutualfund.com/"}
	logo_map[12180.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/jm_financial_angel.svg", "https://www.jmfinancialmf.com/Downloads/Addenda.aspx?SubReportID=538A8B27-62B1-4CFE-BFF2-65DDFF0D4627"}
	logo_map[18076.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/sundaram_angel.svg", "https://www.sundarammutual.com/Downloads"}
	logo_map[27298.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/quantum_angel.svg", "https://www.quantumamc.com/application-transaction-forms/11"}
	logo_map[23411.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/uti_angel.svg", "https://utimf.com/forms-and-downloads/"}
	logo_map[75946.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/nj_mutual_fund_angel.svg", ""}
	logo_map[41331.0] = &[2]string{"NA", "https://www.iiflmf.com/downloads/disclosures"}
	logo_map[17955.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/franklin_templeton_investments_angel.svg", "https://www.franklintempletonindia.com/investor/downloads/fund-documents"}
	logo_map[43539.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/ppfas_angel.svg", "https://amc.ppfas.com/downloads/"}
	logo_map[75945.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/samco_mutual_angel.svg", ""}
	logo_map[18585.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/dsp_investment_angel.svg", "https://www.dspim.com/downloads"}
	logo_map[73211.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/iti_mutual_angel.svg", "https://www.itiamc.com/downloads"}
	logo_map[41236.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/union_bank_angel.svg", "https://www.unionmf.com/downloads/schemeinformationdoc.aspx"}
	logo_map[18445.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/tauras_angel.svg", "https://www.taurusmutualfund.com/sid"}
	logo_map[29993.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/Edelwise_angel.svg", "https://www.edelweissmf.com/Download/scheme-information-document.aspx"}
	logo_map[75267.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/trust_mutual_fund_angel.svg", ""}
	logo_map[5431.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/tata_angel.svg", "https://www.tatamutualfund.com/downloads/"}
	logo_map[73700.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/white_oak_angel.svg", ""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	logo_map[3583.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/icici_angel.svg", "https://www.icicipruamc.com/downloads"}
	logo_map[14964.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/quant_angel.svg", "https://quantmutual.com/downloads/kim"}
	logo_map[35448.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/axis_angel.svg", "https://www.axismf.com/downloads"}
	logo_map[19712.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/nippon_angel.svg", "https://mf.nipponindiaim.com/investor-service/downloads/scheme-information-document"}
	logo_map[21273.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/hdfc_angel.svg", "https://www.hdfcfund.com/investor-desk/product-literature/sid"}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	// logo_map[] ={"NA",""}
	logo_map[6011.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/shriram_angel.svg", "https://www.shriramamc.in/DownloadsFundwise.aspx"}
	logo_map[20327.0] = &[2]string{"https://d3usff6y6s0r8b.cloudfront.net/kotak_angel.svg", "https://www.kotakmf.com/Information/forms-and-downloads"}

	if logo_map[cocode] != nil {
		return logo_map[cocode][0], logo_map[cocode][1], ""
	} else {
		return "", "", "not found"
	}
}
