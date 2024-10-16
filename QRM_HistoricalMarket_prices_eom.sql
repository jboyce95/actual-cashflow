/*
SERVER: AWWAPULSSQLDP01
DATABASE: qrm_pulsar
*/
WITH LastDays AS (
    SELECT 
        EOMONTH(q.[Date]) AS MonthEnd,  
        MAX(q.[Date]) AS LastBusinessDay
    FROM qrm_pulsar.dbo.hist_market q
    GROUP BY EOMONTH(q.[Date])
)
SELECT
    ld.MonthEnd as qrm_month_end
	,ld.LastBusinessDay as qrm_last_business_day
	,q.[Prev Date] as qrm_prev_date
	,q.[GN30 20 Price] as qrm_GN30_20_px_eom
	,q.[GN30 25 Price] as qrm_GN30_25_px_eom
	,q.[GN30 30 Price] as qrm_GN30_30_px_eom
	,q.[GN30 35 Price] as qrm_GN30_35_px_eom
	,q.[GN30 40 Price] as qrm_GN30_40_px_eom
	,q.[GN30 45 Price] as qrm_GN30_45_px_eom
	,q.[GN30 50 Price] as qrm_GN30_50_px_eom
	,q.[GN30 55 Price] as qrm_GN30_55_px_eom
	,q.[GN30 60 Price] as qrm_GN30_60_px_eom
	,q.[GN30 65 Price] as qrm_GN30_65_px_eom
	,q.[GN30 70 Price] as qrm_GN30_70_px_eom
	,q.[UST 10Y] as qrm_UST_10Y_px_eom
	,q.PMMS30 as qrm_PMMS_px_eom
	,CONVERT(VARCHAR(6), ld.[MonthEnd],112) as meperiod_qrm_market
	--,q.[GN30 70 Price] AS LastPrice  
FROM
    LastDays ld
INNER JOIN 
    qrm_pulsar.dbo.hist_market q ON ld.LastBusinessDay = q.[Date]

WHERE LD.MonthEnd > '2019-12-31'
ORDER BY
    ld.MonthEnd ASC