
--SERVER: SRV-SQLFC-1

WITH cte_ebo AS (
SELECT 

a.PrimeKey
,B.LoanId as LoanId2
,a.Flag5 as EBO_Investor
,A.PoolId
,b.MEPeriod as LM_MEPeriod
/*
match lm.LossMitStatusCodeId

C-->2, R-->3, A-->1

LossMitigationStatusId
1
2
3
4
5

LossMitigationStatusCode
A
C
R
S
D

LossMitigationStatusDescription
Active
Completed
Removed
Suspended
Deleted
*/

from SPD.ServInvestment.EBO_Pools (nolock) as A

Join SMD.dbo.Loan_Master_ME_SSE (nolock) as B
    on A.LoanId = B.LoanId

where datediff(month, convert(date, concat((A.MEPeriod), '01')) , DATEADD(MONTH, +1, convert(date, concat((B.MEPeriod), '01')) ) ) >=0 --trade_period...starting at buyout when period 0, period one is month bought out
),

--VVVV include CTE of Investor Accounting table VVVV--
cte_ia AS (

SELECT
    a.loanid as SSELoanid
	,LEFT(a.Transactiondatekey,6) AS ia_Transaction_MEPeriod
    ,sum(a.principalcollected) as ia_total_SSEPrincipal
    ,sum(a.totalinterestcollected) as ia_total_SSETotalinterest
    ,sum(a.netinterestcollected) as ia_total_SSENetinterest
    ,sum(a.servicefeecollected) as ia_total_SSEServiceFee ---Net S/F
    ,sum(a.Guarantyfeecollected) as ia_total_SSEGuarantyFee 

FROM SSE_InvestorAccounting.Reporting.LoanLevelAllocationDetailSummary AS a (nolock)

WHERE LEN(a.transactiondatekey) = 8 -- Filter out archived records that are 9 digits long (and start with a 1).
and LEFT(a.Transactiondatekey,6) >= 201812
and a.OrganizationNumber like 'D__' -- 3/6/2024 IA update --so for EBO purposes I'd make a tweak to the ORg Number clause, D series

--AND a.principalcollected > 0 --had to remove this because final liquidation activity was getting missed with "cleanup claims"
and a.TransactionTypeName not like '%Transfer%' -- 3/6/2024 IA update  --id replace the prin >0 with this TransactionTypeName not like "Transfer"

GROUP BY
a.LoanId
,LEFT(a.Transactiondatekey,6)
),

--VVVV include CTE of original trade price VVVV--
cte_orig_px as (

SELECT 

cast(sale.LoanID as bigint) as LoanId
,sale.PrimeKey as PrimeKey_pbo
,sale.SaleMonth
,sale.Investor as Investor_pbosale
,sale.GoSSplit as GoSSplit_pbosale
,sale.SaleUPB as PBOSale_SaleUPB_Orig
,sale.SalePassThru as PBOSale_SalePassThru
,lm.MEPeriod as MEPeriod_pbo
,DATEADD(day, 15, EOMONTH(convert(date, concat((lm.MEPeriod), '01')),0) ) as wire_date_remit
,lm.InvestorId
,rfi.InvestorOwner
,rfi.InvestorName
,lag(rfi.InvestorOwner,1) over (order by sale.PrimeKey, lm.MEPeriod asc) AS InvestorOwner_previous--ADD LEAD AND LAG FIELDS
,lead(rfi.InvestorOwner,1) over (order by sale.PrimeKey, lm.MEPeriod asc) AS InvestorOwner_next--ADD LEAD AND LAG FIELDS
,lm.InvestorCategoryCodeId
,lm.CurrentPrincipalBalanceAmt
,lag(lm.CurrentPrincipalBalanceAmt,1) over (order by sale.PrimeKey, lm.MEPeriod asc) AS CurrentPrincipalBalanceAmt_previous--ADD LEAD AND LAG FIELDS
,lead(lm.CurrentPrincipalBalanceAmt,1) over (order by sale.PrimeKey, lm.MEPeriod asc) AS CurrentPrincipalBalanceAmt_next--ADD LEAD AND LAG FIELDS
,lm.CurrentInterestRate
,CASE
	WHEN lm.FirstServiceFeeRt >= 0.0025 THEN (floor(200*(lm.CurrentInterestRate - lm.FirstServiceFeeRt/2)))/200
	ELSE (floor(200*(lm.CurrentInterestRate - 0.0025)))/200
END AS mbs_pt_rate
,(CASE
	WHEN lm.FirstServiceFeeRt >= 0.0025 THEN (floor(200*(lm.CurrentInterestRate - lm.FirstServiceFeeRt/2)))/200
	ELSE (floor(200*(lm.CurrentInterestRate - 0.0025)))/200
END) * lm.CurrentPrincipalBalanceAmt AS mbs_pt_rate_X_upb
,lm.FHA_DebentureRt
,CASE 
	WHEN isnull(lm.MortgagetypeId, lm.lotypeid) = '1' THEN 'FHA'
	WHEN isnull(lm.MortgagetypeId, lm.lotypeid) = '2' THEN 'VA'
	WHEN isnull(lm.MortgagetypeId, lm.lotypeid) = '9' THEN 'USDA'
	ELSE 'CONV'
END as LoanType
,lm.InterestPaidToDt
,lm.FCStatusCodeId
,lm.LossMitStatusCodeId
,lm.LossMitTemplateId
,lm.PayoffStopCodeId --check --> just include the filters below ('89','1')
,lag(lm.PayoffStopCodeId,1) over (order by sale.PrimeKey, lm.MEPeriod asc) AS PayoffStopCodeId_previous--ADD LEAD AND LAG FIELDS
,lead(lm.PayoffStopCodeId,1) over (order by sale.PrimeKey, lm.MEPeriod asc) AS PayoffStopCodeId_next--ADD LEAD AND LAG FIELDS
,lm.PayoffReasonCodeId --check --> just include the filters below ('FS', 'RR', 'PF')
,lm.DelinquentPaymentCount --ADDED PER AW REQUEST 8/21/2024
,CASE
	WHEN LM.NextPaymentDueDt > CONVERT(date, convert(varchar,(lm.MEPeriod*100)+'01'),112) then 0 -- this removes dq_months < 0
	ELSE datediff(m,lm.NextPaymentDueDt,CONVERT(CHAR(10), dateadd(m,1,CONVERT(date, convert(varchar,(lm.MEPeriod*100)+'01'),112)), 120)) 
END AS dq_months --ADDED PER AW REQUEST 8/21/2024
,lm.LoanStatusId --check --> just include the filters below ('F','L')
,lm.LoanSubStatusId
,lag(lm.LoanSubStatusId,1) over (order by sale.PrimeKey, lm.MEPeriod asc) AS LoanSubStatusId_previous--ADD LEAD AND LAG FIELDS
--these lagging LoanSubStatusId fields will be used to see when PENDING loans would have been redelivered
,lag(lm.LoanSubStatusId,2) over (order by sale.PrimeKey, lm.MEPeriod asc) AS LoanSubStatusId_previous2
,lag(lm.LoanSubStatusId,3) over (order by sale.PrimeKey, lm.MEPeriod asc) AS LoanSubStatusId_previous3
,lag(lm.LoanSubStatusId,4) over (order by sale.PrimeKey, lm.MEPeriod asc) AS LoanSubStatusId_previous4
,lag(lm.LoanSubStatusId,5) over (order by sale.PrimeKey, lm.MEPeriod asc) AS LoanSubStatusId_previous5
,lag(lm.LoanSubStatusId,6) over (order by sale.PrimeKey, lm.MEPeriod asc) AS LoanSubStatusId_previous6
,lead(lm.LoanSubStatusId,1) over (order by sale.PrimeKey, lm.MEPeriod asc) AS LoanSubStatusId_next
,CASE 
	WHEN lm.LoanSubStatusId ='CP' AND
	lag(lm.LoanSubStatusId,1) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' AND
	lag(lm.LoanSubStatusId,2) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' AND
	lag(lm.LoanSubStatusId,3) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' THEN 1
	ELSE 0
END AS loan_status_cc4_flag
,CASE 
	WHEN lm.LoanSubStatusId ='CP' AND
	lag(lm.LoanSubStatusId,1) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' AND
	lag(lm.LoanSubStatusId,2) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' AND
	lag(lm.LoanSubStatusId,3) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' AND
	lag(lm.LoanSubStatusId,4) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' AND
	lag(lm.LoanSubStatusId,5) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' AND
	lag(lm.LoanSubStatusId,6) over (order by sale.PrimeKey, lm.MEPeriod asc) ='CP' THEN 1
	ELSE 0
END AS loan_status_cc7_flag
,lm.CurrentMaturityTerm --ADDED PER AW REQUEST 3/19/2024
,lm.CurrentMonthlyPaymentAmt --PandI amount for use in PT savings
,lm.FirstServiceFeeRt -- subtract sfee from PandI amount for use in PT savings
--,round(lm.currentprincipalbalanceamt * lm.firstservicefeert / 12,2) as sfee_amount -- subtract sfee_amount from PandI amount for use in PT savings
,CAST(right(LM.LoanStatusString,12) AS VARCHAR(MAX)) AS LoanStatusString_12mo
,CASE
	WHEN right(lm.LoanStatusString,7)='0000000' THEN '7+'
	WHEN right(lm.LoanStatusString,6)='000000' THEN '6'
	WHEN right(lm.LoanStatusString,5)='00000' THEN '5'
	WHEN right(lm.LoanStatusString,4)='0000' THEN '4'
	WHEN right(lm.LoanStatusString,3)='000' THEN '3'
	WHEN right(lm.LoanStatusString,2)='00' THEN '2'
	WHEN right(lm.LoanStatusString,1)='0' THEN '1'
	ELSE '0'
	END AS 'clean_current_payments'
,datediff(month, convert(date, concat((sale.SaleMonth), '01')) , DATEADD(MONTH, +1, convert(date, concat((lm.MEPeriod), '01')) ) ) as trade_period

FROM SPD.ServInvestment.PBOSale sale

LEFT JOIN SMD..Loan_Master_ME_SSE lm ON SALE.LoanID = lm.LoanId --and convert(varchar(6),dateadd(m,-1,CONVERT(datetime,CONVERT(varchar(8),sale.SaleDate,112),112)),112) = lm.MEPeriod
JOIN SMD..ref_Investor_ME_SSE rfi on LM.InvestorId = rfi.InvestorId and lm.MEPeriod = rfi.Meperiod

where 1=1

and datediff(month, convert(date, concat((sale.SaleMonth), '01')) , DATEADD(MONTH, +1, convert(date, concat((lm.MEPeriod), '01')) ) ) >=0
),

cte_orig_px2 as (

SELECT 

cast(sale.LoanID as bigint) as LoanId2
,sale.PrimeKey as PrimeKey_pbo2
,sale.Investor as Investor_pbosale2
,sale.Price as PBOSale_Price
,sale.SaleUPB as PBOSale_SaleUPB
,sale.SalePassThru as PBOSale_SalePassThru2
,sale.SaleDate as PBOSale_SaleDate_OLD
,CONVERT(date, CONVERT(varchar(8), sale.SaleDate), 112) as PBOSale_SaleDate 
,sale.IntPaidToDate as PBOSale_IntPaidToDate
,convert(varchar(6),dateadd(m,-1,CONVERT(datetime,CONVERT(varchar(8),sale.SaleDate,112),112)),112) as me_tradeperiod_0
,CASE 
	WHEN isnull(lm.MortgagetypeId, lm.lotypeid) = '1' THEN 'FHA'
	WHEN isnull(lm.MortgagetypeId, lm.lotypeid) = '2' THEN 'VA'
	WHEN isnull(lm.MortgagetypeId, lm.lotypeid) = '9' THEN 'USDA'
	ELSE 'CONV'
END as LoanType_2

FROM SPD.ServInvestment.PBOSale sale

LEFT JOIN SMD..Loan_Master_ME_SSE lm ON SALE.LoanID = lm.LoanId and convert(varchar(6),dateadd(m,-1,CONVERT(datetime,CONVERT(varchar(8),sale.SaleDate,112),112)),112) = lm.MEPeriod
),

-- VVVV ADD CUMULATIVE PRIN AND INT VVVV
cte_ia_fc as (
select

pt.loanid as loanid_ia_fc
,cast(pt.TransactionDate as date) as TransactionDate
,sum(pt.PrincipalAmount) as IA_PrincipalAmount_FC
,sum(pt.InterestAmount) as IA_InterestAmount_FC
,lm.MEPeriod as meperiod_ia_fc

from SSE_LoanServicing.Transactions.PaymentTransactions(NOLOCK) pt
left join smd..loan_master_me_sse lm on pt.loanid=lm.LoanId and CONVERT(varchar(6),pt.Transactiondate,112)=lm.MEPeriod

where (
	pt.BatchNumber between '5RA' and '5RJ'--EBO Transactions
	or pt.BatchNumber between '4D1' and '4D9'--Claims Transactions
	)
and lm.InvestorId not like '4__' -- to correct duplicate meperiods when loan is redelivered, which is not a FC or SSDIL

group by pt.loanid, lm.MEPeriod, pt.transactiondate
/*
This is the list of EBO Batch Numbers from Jared
TABLE:	SSE_LoanServicing.Transactions.PaymentTransactions
Column:	BatchNumber
	5RA-->	EBO Liquidations - Short Sale-Check
	5RB-->	EBO Liquidations - Short Sale-Wire-BofA
	5RC-->	EBO Liquidations - 3rd Party FCL Sale-Check
	5RD-->	EBO Liquidations - 3rd Party FCL Sale-Wire-BofA
	5RE-->	EBO Liquidations - REO-Check
	5RF-->	EBO Liquidations - REO-Wire-BofA
	5RG-->	EBO Liquidations - Redemption-Check
	5RH-->	EBO Liquidations - Redemption-Check
	5RI-->	EBO Liquidations - CWCOT-Wire-BofA
	5RJ-->	EBO Liquidations - CWCOT-Check
	
	4D1    4D4    Claims Processing - Check
	4D5    4D9    Claims Processing - Wires
	4D1-4D4 Check     4D5-4D9 Wire
*/
),

cte_ebo_forecast as (	
select 
		 NUll as vector_Pool,
		 Flag1 as vector_BuyoutMonth,
--         Period as trade_period, --EBOAge,
         Period as vector_ebo_age,
		 Flag9 as vector_Investor,
         ModVector as vector_EstModRate,
         CureVector as vector_EstCureRate,
         PIFVector as vector_EstPrepayRate,
		 FCLVector as vector_EstFCLRate,
		 SS_DILVector as vector_EstSSDILRate
 
from  spd.servinvestment.EBO_Model_LL

union 

select 
		 NUll as vector_Pool,
		 Flag1 as vector_BuyoutMonth,
--         Period as trade_period, --EBOAge,
         Period as vector_ebo_age,
		 'NB' as vector_Investor,
         Mod as vector_EstModRate,
         Cure as vector_EstCureRate,
         PIF as vector_EstPrepayRate,
		 FCL as vector_EstFCLRate,
		 SS_DIL as vector_EstSSDILRate

from  spd.servinvestment.EBO_Model_NB (nolock)

union

select 
		 Null as Pool, 
		 BuyoutMonth,
--         Period as trade_period, --EBOAge,
         Period as vector_ebo_age,
		 'PBO' as vector_Investor,
         ModVector as vector_EstModRate,
         CureVector as vector_EstCureRate,
         PIFVector as vector_EstPrepayRate,
		 FCLVector as vector_EstFCLRate,
		 SS_DILVector as vector_EstSSDILRate

from  spd.servinvestment.EBO_Model_PBO (nolock)
)

SELECT 

cte_orig_px.PrimeKey_pbo
,cte_orig_px.SaleMonth
,cte_orig_px.LoanID as LoanId
,cte_orig_px.Investor_pbosale
,cte_orig_px.PBOSale_SalePassThru
,cte_orig_px.PBOSale_SaleUPB_Orig
,cte_orig_px.GoSSplit_pbosale
,cte_orig_px.MEPeriod_pbo
,cte_orig_px.trade_period
,cte_orig_px.wire_date_remit
,cte_orig_px.InvestorId
,cte_orig_px.InvestorOwner
,cte_orig_px.InvestorName
,cte_orig_px.InvestorOwner_previous
,cte_orig_px.InvestorOwner_next
,cte_orig_px.InvestorCategoryCodeId
,cte_orig_px.CurrentPrincipalBalanceAmt
,cte_orig_px.CurrentPrincipalBalanceAmt_previous
,cte_orig_px.CurrentPrincipalBalanceAmt_next
,cte_orig_px.CurrentInterestRate
,cte_orig_px.mbs_pt_rate
,cte_orig_px.mbs_pt_rate_X_upb
,cte_orig_px.FirstServiceFeeRt
,cte_orig_px.FHA_DebentureRt
,cte_orig_px.LoanType
,cte_orig_px.InterestPaidToDt
,cte_orig_px.FCStatusCodeId
,cte_orig_px.LossMitStatusCodeId
,cte_orig_px.LossMitTemplateId
,cte_orig_px.PayoffStopCodeId --check --> just include the filters below ('89','1')
,cte_orig_px.PayoffStopCodeId_previous
,cte_orig_px.PayoffStopCodeId_next
,cte_orig_px.PayoffReasonCodeId --check --> just include the filters below ('FS', 'RR', 'PF')
,cte_orig_px.DelinquentPaymentCount
,cte_orig_px.dq_months
,cte_orig_px.dq_months * cte_orig_px.CurrentPrincipalBalanceAmt as dq_months_X_UPB
,cte_orig_px.LoanStatusId --check --> just include the filters below ('F','L')
,cte_orig_px.LoanSubStatusId
,cte_orig_px.LoanSubStatusId_previous
,cte_orig_px.LoanSubStatusId_previous2
,cte_orig_px.LoanSubStatusId_previous3
,cte_orig_px.LoanSubStatusId_previous4
,cte_orig_px.LoanSubStatusId_previous5
,cte_orig_px.LoanSubStatusId_previous6
,cte_orig_px.LoanSubStatusId_next
,cte_orig_px.loan_status_cc4_flag
,cte_orig_px.loan_status_cc7_flag
,CASE 
	WHEN ((cte_orig_px.MEPeriod_pbo>=202302 and cte_orig_px.loan_status_cc4_flag=1) or (cte_orig_px.MEPeriod_pbo<202302 and cte_orig_px.loan_status_cc7_flag=1)) THEN 1
	ELSE 0
END AS redelivery_elig_flag -- This flag shows whether a loan was eligible for redelivery based on the seasoning requirements before and after February 2023
,cte_orig_px.CurrentMaturityTerm
-- OLD VERSION ,cte_orig_px.CurrentMonthlyPaymentAmt - sfee_amount as penny_pt_savings -- <<<< PENDING...need to bring in sfee to mult x currentupb... monthly payment minus savings --FirstServiceFeeRt
-- ^^^^ change this to calc sfee amount based on previous upb
-- vvvv new penny_pt_savings calc
,round( (cte_orig_px.CurrentPrincipalBalanceAmt_previous * cte_orig_px.CurrentInterestRate/12), 2) as payment_interest_only
,round( (cte_orig_px.CurrentMonthlyPaymentAmt - (cte_orig_px.CurrentPrincipalBalanceAmt_previous * cte_orig_px.CurrentInterestRate/12) ), 2) as payment_principal_only
,round( (cte_orig_px.CurrentPrincipalBalanceAmt_previous * (cte_orig_px.CurrentInterestRate - cte_orig_px.firstservicefeert)/12), 2) as payment_net_interest_only
,round(cte_orig_px.CurrentPrincipalBalanceAmt_previous * cte_orig_px.firstservicefeert / 12,2) as sfee_amount
,CASE 
	WHEN cte_orig_px.trade_period = 0 THEN 0
	ELSE cte_orig_px.CurrentMonthlyPaymentAmt - round(cte_orig_px.CurrentPrincipalBalanceAmt_previous * cte_orig_px.firstservicefeert / 12,2) 
END AS penny_pt_savings_orig
-- ^^^^ new penny_pt_savings calc
-- subtract sfee_amount from PandI amount for use in PT savings
,cte_orig_px.clean_current_payments
,cte_ebo.*
,ia_total_SSEPrincipal
,sum(ia_total_SSEPrincipal) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) as cumulative_principal_collected_ia
,ia_total_SSETotalinterest
,sum(ia_total_SSETotalinterest) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) as cumulative_interest_collected_ia
,ia_total_SSENetinterest
,sum(ia_total_SSENetinterest) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) as cumulative_netinterest_collected_ia
,ia_total_SSEServiceFee
,sum(ia_total_SSEServiceFee) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) as cumulative_sfee_collected_ia
,ia_total_SSEGuarantyFee 
-- VVVV ADD CUMULATIVE PRIN AND INT VVVV
,cte_ia_fc.IA_PrincipalAmount_FC
,cte_ia_fc.IA_InterestAmount_FC
,sum(IA_PrincipalAmount_FC) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) as ia_cumulative_principal_fc --what's unbounded, preceding, current row mean
,sum(IA_InterestAmount_FC) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and 1 preceding) as ia_cumulative_total_interest_fc --get the cumulative interest from the previous period rather than current row. Principal will use current row since we're calculating the interest from this total principal amount
,CASE 
	WHEN cte_orig_px.LoanStatusId='L' THEN 
		sum(IA_PrincipalAmount_FC) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) 
		- cte_ia_fc.IA_PrincipalAmount_FC
	ELSE 0
END AS remit_sales_proceeds
,CASE 
	when cte_orig_px.LoanStatusId='L' THEN 
		CASE when cte_orig_px.CurrentPrincipalBalanceAmt_previous - cte_ia_fc.IA_PrincipalAmount_FC >1 then 
			sum(IA_PrincipalAmount_FC) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) 
			+ cte_orig_px.CurrentPrincipalBalanceAmt_previous - cte_ia_fc.IA_PrincipalAmount_FC
		ELSE cte_orig_px.CurrentPrincipalBalanceAmt + sum(IA_PrincipalAmount_FC) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) 
		END
	ELSE 0
	END AS remit_upb_preliquidation
,CASE 
	WHEN cte_orig_px.LoanStatusId='L' THEN cte_orig_px.CurrentPrincipalBalanceAmt_previous
	ELSE 0
END AS remit_expected_claims_upb
,CASE
	WHEN cte_orig_px.LoanType='FHA' AND cte_ia_fc.loanid_ia_fc is not null THEN DATEADD(MONTH,2,cte_orig_px.InterestPaidToDt)
	WHEN cte_orig_px.LoanType in ('VA','USDA') AND cte_ia_fc.loanid_ia_fc is not null THEN cte_orig_px.InterestPaidToDt
	ELSE NULL
END as remit_default_dt
,min(TransactionDate) over (partition by cte_orig_px.PrimeKey_pbo, cte_ia_fc.loanid_ia_fc) as remit_MinTransactiondate
,max(TransactionDate) over (partition by cte_orig_px.PrimeKey_pbo, cte_ia_fc.loanid_ia_fc) as remit_MaxTransactiondate
,EOMONTH(TransactionDate) as remit_eom_TransactionDate
,CASE
	WHEN cte_ia_fc.loanid_ia_fc is not null AND (
		sum(IA_PrincipalAmount_FC) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) --similar to the cumulative ia_prin logic
		- cte_ia_fc.IA_PrincipalAmount_FC >=1) THEN CASE
		WHEN cte_orig_px.LoanType='FHA' THEN DATEDIFF(DAY,cte_orig_px.InterestPaidToDt,(min(TransactionDate) over (partition by cte_orig_px.PrimeKey_pbo, cte_ia_fc.loanid_ia_fc))) - 60 -- using MinTransactiondate logic
		ELSE DATEDIFF(DAY,cte_orig_px.InterestPaidToDt,(min(TransactionDate) over (partition by cte_orig_px.PrimeKey_pbo, cte_ia_fc.loanid_ia_fc))) -- using MinTransactiondate logic
		END
	END AS remit_days_split_interest
,CASE
-- VVVV try using the cumulative sum
	WHEN cte_ia_fc.loanid_ia_fc is not null AND (
		sum(IA_PrincipalAmount_FC) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) --similar to the cumulative ia_prin logic
		- cte_ia_fc.IA_PrincipalAmount_FC >=1) THEN DATEDIFF(DAY,(min(TransactionDate) over (partition by cte_orig_px.PrimeKey_pbo, cte_ia_fc.loanid_ia_fc)),EOMONTH(TransactionDate)) --switched order of dates
	WHEN cte_ia_fc.loanid_ia_fc is not null AND (
	sum(IA_PrincipalAmount_FC) over (PARTITION BY cte_orig_px.PrimeKey_pbo order by cte_orig_px.PrimeKey_pbo, cte_orig_px.MEPeriod_pbo asc rows between unbounded preceding and current row) --this is the cumulative ia_prin logic
--NEED FHA CASE
	- cte_ia_fc.IA_PrincipalAmount_FC <1) THEN CASE
			WHEN cte_orig_px.LoanType='FHA' THEN DATEDIFF(DAY,cte_orig_px.InterestPaidToDt,EOMONTH(TransactionDate)) - 60 --need to subtract 60 if FHA
			ELSE DATEDIFF(DAY,(min(TransactionDate) over (partition by cte_orig_px.PrimeKey_pbo, cte_ia_fc.loanid_ia_fc)),EOMONTH(TransactionDate))
		END 
END AS remit_days_default_dt
,cte_orig_px2.PrimeKey_pbo2
,cte_orig_px2.Investor_pbosale2
,cte_orig_px2.PBOSale_Price
,cte_orig_px2.PBOSale_SaleUPB
,cte_orig_px2.PBOSale_SalePassThru2
,cte_orig_px2.PBOSale_SaleDate
,cte_orig_px2.PBOSale_IntPaidToDate
,cte_orig_px2.me_tradeperiod_0
,cte_orig_px2.LoanType_2
,CASE 
	WHEN cte_orig_px.PayoffReasonCodeId='PF' AND ia_total_SSEPrincipal>0 THEN 'PIF'
	ELSE 'NULL'--0
	END AS path_pif
,CASE 
	WHEN ((cte_orig_px.LoanSubStatusId IN ('LSS', 'LDL') and ia_total_SSEPrincipal>0) OR (cte_orig_px.LoanSubStatusId_previous IN ('LSS', 'LDL') and ia_total_SSEPrincipal>0)) THEN 'SSDIL' --'DL' TOO?? --<<<< PENDING ADD OR STATEMENT FOR NEXT PERIOD WITH LSS OR LDL
	WHEN cte_orig_px.LoanSubStatusId IN ('3F', 'LFC', 'LU', 'LUS', 'LRS') and ia_total_SSEPrincipal>0 THEN 'FC' --added 'LRS' 2024.08.09; 'FS' TOO?? NO
	ELSE 'NULL'
	END AS path_liq
,case 
	when cte_orig_px.Investor_pbosale in ('MM', 'TO') THEN 'MM'
	ELSE cte_orig_px.Investor_pbosale
END AS investor_pbosale_vector_pivot
,CASE
	WHEN cte_orig_px.clean_current_payments IN ('4', '5', '6', '7+') THEN 1
	ELSE 0
END AS clean_and_current_4mo
,CASE
	WHEN cte_orig_px.clean_current_payments IN ('7+') THEN 1
	ELSE 0
END AS clean_and_current_7mo

--FROM cte_ebo
FROM cte_orig_px

LEFT JOIN cte_ebo on cte_orig_px.LoanId = cte_ebo.LoanId2 and cte_orig_px.MEPeriod_pbo = cte_ebo.LM_MEPeriod and cte_orig_px.PrimeKey_pbo = cte_ebo.PrimeKey --this one caused issues-- DONE
LEFT JOIN cte_ia on cte_orig_px.LoanId = cte_ia.SSELoanid and cte_orig_px.MEPeriod_pbo = cte_ia.ia_Transaction_MEPeriod -- DONE
LEFT JOIN cte_orig_px2 on cte_orig_px.LoanId = cte_orig_px2.LoanId2 and (cte_orig_px.PrimeKey_pbo = cte_orig_px2.PrimeKey_pbo2) and (cte_orig_px.MEPeriod_pbo = cte_orig_px2.me_tradeperiod_0) --cte_ebo.LM_MEPeriod = cte_trans_trial.MEPeriod
-- VVVV ADD CUMULATIVE PRIN AND INT FOR LIQ VVVV
LEFT JOIN cte_ia_fc on cte_orig_px.LoanId = cte_ia_fc.loanid_ia_fc and cte_orig_px.MEPeriod_pbo = cte_ia_fc.meperiod_ia_fc AND (cte_orig_px.LoanStatusId not in ('P'))-- LoanStatusId filter fixes duplicate records for claims when loan is current
--LEFT JOIN cte_ebo_forecast on cte_orig_px.SaleMonth=cte_ebo_forecast.vector_BuyoutMonth and cte_orig_px.Investor_pbosale=cte_ebo_forecast.vector_Investor and cte_orig_px.trade_period=cte_ebo_forecast.vector_ebo_age
--LEFT JOIN cte_ebo_forecast on cte_orig_px.SaleMonth=cte_ebo_forecast.vector_BuyoutMonth and ((case when cte_orig_px.Investor_pbosale='TO' then 'MM' Else cte_orig_px.Investor_pbosale end) = cte_ebo_forecast.vector_Investor) and cte_orig_px.trade_period=cte_ebo_forecast.vector_ebo_age
--(case when A.Investor='PBO-FEMA' then 'NB' Else A.Investor end)=B.Investor 
where 1=1
--where SaleMonth>=201901 --had 3.7mm records for 202404 ME
--and SaleMonth>=202101 --had 2.1mm records for 202404 ME
and SaleMonth>=202203 --Approximate period that SSE migration completed
and not (cte_orig_px.CurrentPrincipalBalanceAmt=0 and CurrentPrincipalBalanceAmt_previous=0 and cte_orig_px.LoanSubStatusId IN ('3F', 'LFC', 'LU', 'LUS','LSS', 'LDL', 'LRS'))
--and not (cte_orig_px.InvestorOwner_previous ='GNMA' AND cte_orig_px.InvestorOwner ='GNMA' AND cte_orig_px.InvestorOwner_next='GNMA') --remove most redelivery periods; have to keep some due to PBO pathing; clean up in python
and not (cte_orig_px.InvestorOwner_previous ='GNMA' AND 
	cte_orig_px.InvestorOwner ='GNMA' AND 
	cte_orig_px.InvestorOwner_next='GNMA' AND
	cte_orig_px.trade_period>1) -- to fix loans that are redelivered the same month they settled
and not (cte_orig_px.InvestorOwner_previous ='GNMA' AND 
	cte_orig_px.InvestorOwner ='GNMA' AND
	cte_orig_px.InvestorOwner_next ='PLS' AND -- this keeps trade_period 0 in the results but removes post-redelivery periods
	(cte_orig_px.Investor_pbosale in ('MM', 'NB', 'GS', 'BV', 'TO', 'AP', 'BB'))) --remove most redelivery periods; have to keep some due to PBO pathing; clean up in python
and not (cte_orig_px.InvestorOwner_previous ='GNMA' AND 
	cte_orig_px.InvestorOwner ='PLS' AND
	cte_orig_px.InvestorOwner_next ='GNMA' AND -- this keeps trade_period 0 in the results but removes post-redelivery periods
	(cte_orig_px.Investor_pbosale in ('MM', 'NB', 'GS', 'BV', 'TO', 'AP', 'BB'))) --remove most redelivery periods; have to keep some due to PBO pathing; clean up in python
and cte_orig_px.InvestorCategoryCodeId <> '099' --to remove unnecessary post liquidation periods
and not (cte_orig_px.CurrentPrincipalBalanceAmt=0 and CurrentPrincipalBalanceAmt_previous=0 and cte_orig_px.PayoffReasonCodeId IN ('PF')) --remove periods post PIF
--and cte_orig_px.LoanId in ('1000359649'	,'1001985204'	,'1002367642'	,'1002647086'	,'1004001501'	,'1004013254'	,'1005225011'	,'1005325622'	,'1006491109'	,'1006929105'	,'8004307147'	,'8006497629'	,'8010456791'	,'8012324654'	,'8013412288'	,'8013577360'	,'8017849020'	,'8018503817'	,'8020546574'	,'8022965865'	,'8023110500'	,'8024274980'	,'8026672718'	,'8027193294'	,'8028184192'	,'8030828231'	,'8010715848'	,'8016759370'	,'8016941265'	,'8018132289'	,'8023639752')
	-- ^^^^ Cures and Mods from 202203 and 202204 trades - where prin doesn't tie ^^^^
--and cte_orig_px.LoanId in ('1000154904'	,'1000170928'	, '1000194906')

order by SaleMonth, cte_orig_px.LoanId, trade_period, MEPeriod_pbo  --cte_orig_px.PrimeKey_pbo, 
