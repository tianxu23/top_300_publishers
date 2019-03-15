drop table top300;


create multiset volatile table top300 as
(
sel
      CASE WHEN fam.AMS_PRGRM_ID IN (1) THEN 'US'                    
		WHEN fam.AMS_PRGRM_ID = 4 THEN 'AU'                    
		WHEN fam.AMS_PRGRM_ID = 11 THEN 'DE'                    
		WHEN fam.AMS_PRGRM_ID = 15 THEN 'UK'                    
		WHEN fam.AMS_PRGRM_ID =10 THEN 'FR'                    
		WHEN fam.AMS_PRGRM_ID=12 THEN 'IT'                    
		WHEN fam.AMS_PRGRM_ID=13 THEN 'ES'                    
		WHEN fam.AMS_PRGRM_ID IN (2,3,5,14,16) THEN 'ROE'                                        
		END AS region,
	 
	   fam.EPN_PBLSHR_ID as pblshr_id,
       PBLSHR_CMPNY_NAME as pblshr_name,
	   CASE WHEN PBLSHR_BSNS_MODEL_NAME = 'Loyalty / Incentive' THEN 'Loyalty' 
          WHEN PBLSHR_BSNS_MODEL_NAME = 'Shopping Comparison' THEN 'OCS'
          WHEN PBLSHR_BSNS_MODEL_NAME IN ('Editorial Content','User Generated Content') THEN 'Content'
          WHEN PBLSHR_BSNS_MODEL_NAME = 'Coupons' THEN 'Coupon'
		  WHEN PBLSHR_BSNS_MODEL_NAME = 'Downloadable Tools' THEN 'Tools'
		  ELSE 'Other' END AS fam_bm,
	   CASE WHEN l2.MANUAL_BM= 'Loyalty' THEN 'Loyalty' 
	    WHEN  l2.MANUAL_BM = 'Shopping Comparison' THEN 'OCS'
		 WHEN  l2.MANUAL_BM = 'Content' THEN 'Content'
		 WHEN l2.MANUAL_BM = 'Coupon' THEN 'Coupon'
		  WHEN l2.MANUAL_BM ='Tools' THEN 'Tools'
	    ELSE 'Other' END as l2_v_bm,
	   CASE WHEN bm.MANUAL_BM= 'Loyalty' THEN 'Loyalty' 
	    WHEN  bm.MANUAL_BM = 'Shopping Comparison' THEN 'OCS'
		 WHEN  bm.MANUAL_BM = 'Content' THEN 'Content'
		 WHEN bm.MANUAL_BM = 'Coupon' THEN 'Coupon'
		 WHEN bm.MANUAL_BM ='Tools' THEN 'Tools'
	    ELSE 'Other' END  as chengliu_bm,
		CASE WHEN l2_v_bm = chengliu_bm THEN 'Y'
		ELSE 'N' END as l2_vs_chengliu,
		CASE WHEN fam_bm = l2_v_bm THEN 'Y'
		ELSE 'N' END as fam_vs_l2,
		CASE WHEN fam_bm = l2_v_bm  and fam_bm = chengliu_bm THEN 'Y'
		ELSE 'N' END as all_equal,
		 sum(fam.IGMB_PLAN_RATE_AMT)  as igmb,
	  rank() over(partition by  region order by igmb desc) as pblshr_rank
from 
       PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT fam
left join 
       prs_ams_v.ams_pblshr pblshr
       on fam.EPN_PBLSHR_ID = pblshr.AMS_PBLSHR_ID
left join 
       prs_ams_v.AMS_PBLSHR_BSNS_MODEL pb_bm
       on pb_bm.PBLSHR_BSNS_MODEL_ID = coalesce(pblshr.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, pblshr.PBLSHR_BSNS_MODEL_ID, -999)
left join
       APP_MRKTNG_L2_V.New_bm l2
      on l2.AMS_PBLSHR_ID = fam.EPN_PBLSHR_ID
left join 
       p_chengliu_t.new_BM bm
       on bm.AMS_PBLSHR_ID =  fam.EPN_PBLSHR_ID
where 
       ck_trans_dt >= '2018-01-01'
	   and 
	   region in ('US','AU','DE','UK','FR','IT','ES','ROE')
	   and pblshr_id <> -999
group by 1,2,3,4,5,6,7,8,9
qualify row_number() over(partition by  region order by igmb desc)<= 300
)
with data primary index( pblshr_id ) on commit preserve rows;

sel * from  top300;
	
	
sel 	region,
all_equal,
count(all_equal)
from top300
group by 1, l2_vs_chengliu,fam_vs_l2, all_equal

;
	
	
	
	
	
	
		CASE WHEN fam_bm = l2_v_bm  and fam_bm = chengliu_bm THEN 'Y'
		ELSE 'N' END as equal_or_not
	
	
	sel * from prs_ams_v.ams_pblshr;
	sel * from PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT  where ck_trans_dt> current_date - 4;
	sel  MANUAL_BM  from APP_MRKTNG_L2_V.New_bm;
	sel MANUAL_BM, count( MANUAL_BM)  from p_chengliu_t.new_BM group by 1;
	sel PBLSHR_BSNS_MODEL_NAME, count(PBLSHR_BSNS_MODEL_NAME) from prs_ams_v.AMS_PBLSHR_BSNS_MODEL group by 1;