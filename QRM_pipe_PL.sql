--SERVER: AWWAPULSSQLDP01

SELECT

q.[Loan Number]
,q.[Face Amount]
,q.[Issue Date Balance]
,CONVERT(VARCHAR(6), q.[settled date], 112) as YearMonth
,q.[Buy Price]
,q.[Shared Econ]
,cast(dateadd(day,-2,q.[Ready Date]) as date) as repurchase_date_calc -- the avg days from wet to dry was 2
,CASE
	WHEN q.[Shared Econ] = 0 and q.[Buy Price] >= 100 THEN 1.00 
	WHEN q.[Shared Econ] > 0 and q.[Buy Price] > 100 THEN round((q.[Buy Price] - 100),3) / 100
	WHEN q.[Buy Price] < 100 THEN round((q.[Buy Price] - 100),3) / 100
	ELSE 0
END AS investor_redelivery_premium_pct --this is post GoS sharing and accounts for the discrepancy in QRM when Shared Econ of 0 has Buy Price > 100

,CASE
	WHEN q.[Shared Econ] = 0 and q.[Buy Price] >= 100 THEN q.[Face Amount] * 1000
	WHEN q.[Shared Econ] > 0 and q.[Buy Price] > 100 THEN round((q.[Buy Price] - 100),3) * q.[Face Amount] * 10
	WHEN q.[Buy Price] < 100 THEN round((q.[Buy Price] - 100),3) * q.[Face Amount] * 10 
	ELSE 0
END AS investor_redelivery_premium_dlr --this is post GoS sharing and accounts for the discrepancy in QRM when Shared Econ of 0 has Buy Price > 100

-- loan 1000118252 has premium but zero shared econ in the system (which is wrong). 
/*CASE
	WHEN q.[Buy Price] - 100) > 0 and q.[Shared Econ] = 0, THEN --USE THIS TO FIX LOAN 1000118252 */

,CASE
--	WHEN q.[Buy Price] >= 100 THEN round((q.[settled price] - q.[Buy Price]),3) / 100 * ((100 - q.[Shared Econ]) / q.[Shared Econ])
	WHEN q.[Shared Econ] = 0 and q.[Buy Price] >= 100 THEN round((q.[settled price] - 100),3) / 100
	WHEN q.[Shared Econ] = 50 and q.[Buy Price] > 100 THEN round((q.[Buy Price] - 100),3) / 100
	WHEN q.[Shared Econ] < 50 and q.[Buy Price] > 100 THEN round((q.[Buy Price] - 100),3) / 100 * ((100 - q.[Shared Econ]) / q.[Shared Econ])
	WHEN q.[Buy Price] < 100 THEN 0 
	ELSE 0
END AS penny_redelivery_premium_pct
,CASE
	WHEN q.[Shared Econ] = 0 and q.[Buy Price] >= 100 THEN round((q.[settled price] - 100),3) / 100 * (q.[Issue Date Balance] * 1000)
	WHEN q.[Shared Econ] = 50 and q.[Buy Price] > 100 THEN round((q.[Buy Price] - 100),3) / 100 * (q.[Face Amount] * 1000)
	WHEN q.[Shared Econ] < 50 and q.[Buy Price] > 100 THEN (round((q.[Buy Price] - 100),3) / 100 * ((100 - q.[Shared Econ]) / q.[Shared Econ])) * (q.[Face Amount] * 1000)
	WHEN q.[Buy Price] < 100 THEN 0 
	ELSE 0
END AS penny_redelivery_premium_dlr
,q.[Pool Num]
,q.[Management]
,q.[Pool Status]
,q.[Type Dimension]
,q.[Loan Type]
,q.[settled date]
,q.[settled price]
,q.Other as EBO_Investor
,CASE
	WHEN q.[Loan Type] like '%MOD' THEN 'Mod'
	WHEN q.[Loan Type] like '%CVD' THEN 'Cure'
	WHEN q.[Loan Type] like '%CURE' THEN 'Cure'
	ELSE 'N/A'
END AS Path_Reperf


from qrm_pulsar..vw_Pipe_Set q --where [settled date] > '8/1/2021'
--from qrm_pulsar..vw_Pipe_Set q where CONVERT(VARCHAR(6), [settled date], 112) = 202202
WHERE q.[Management] = 'EBO' 
and q.[Pool Status] = 'Settled' 
and q.[Type Dimension] = 'CL'
--and q.[Loan Number] in ('8011644821') --MM loan that redelivered in 202203 2 weeks after settlement
--and q.[Portfolio Date] > '2017-01-01'


--and [Loan Number] in ('1003448726'	,'7001174787'	,'8024806725'	,'1003332030'	,'8015236071'	,'8021169805'	,'1004212987'	,'8019639848'	,'8013658567'	,'8016978129'	,'8030170709'	,'8005999076'	,'8030810742'	,'8030304339'	,'8000070958'	,'1002718217'	,'8007113274'	,'8016469408'	,'8007469124'	,'8022850433'	,'8008420727'	,'1004780991'	,'7002513528'	,'8005843233'	,'1000790774'	,'1000634022'	,'8004134265'	,'1000034173'	,'1000092894'	,'1000169698'	,'1000269817'	,'1000325643'	,'1000351182'	,'1000384500'	,'1000587665'	,'1000886436'	,'1000890506'	,'1001986262'	,'1002229199'	,'1002684195'	,'1002978208'	,'1003119017'	,'1003265829'	,'1003353197'	,'1003367118'	,'1003713735'	,'1003732173'	,'1004030835'	,'1004447295'	,'1004476237'	,'1004619430'	,'1004711286'	,'1005008928'	,'1005013984'	,'1005229909'	,'1005481442'	,'1005557705'	,'1005669219'	,'1005683135'	,'1005714576'	,'1005782344'	,'1005786491'	,'1005959395'	,'1006225716'	,'1006409084'	,'1006721435'	,'1006726517'	,'1006897346'	,'7001854065'	,'8000181662'	,'8000670109'	,'8000939680'	,'8001133619'	,'8001861254'	,'8001905749'	,'8002538433'	,'8002632239'	,'8002753100'	,'8003117195'	,'8003615268'	,'8003668889'	,'8003677152'	,'8003912040'	,'8003930116'	,'8004447277'	,'8005011009'	,'8006014564'	,'8006163795'	,'8006590978'	,'8006813107'	,'8007683070'	,'8007882714'	,'8008077229'	,'8008848230'	,'8009956552'	,'8010700743'	,'8011634255'	,'8012080895'	,'8013038205'	,'8013656207'	,'8013866391'	,'8015669925'	,'8016221104'	,'8016903564'	,'8017230635'	,'8017597092'	,'8017665005'	,'8017776208'	,'8018124629'	,'8018240924'	,'8018434879'	,'8018582813'	,'8018645693'	,'8019282393'	,'8019359507'	,'8019458097'	,'8019818115'	,'8019900452'	,'8020156253'	,'8020188339'	,'8020287564'	,'8020414351'	,'8020611979'	,'8020892807'	,'8021545568'	,'8021920761'	,'8022169893'	,'8022284660'	,'8022298673'	,'8022932986'	,'8023082935'	,'8023091235'	,'8023417296'	,'8023495113'	,'8023559716'	,'8023849975'	,'8023851737'	,'8024715405'	,'8025286311'	,'8025415870'	,'8025908457'	,'8026344251'	,'8026385513'	,'8026623068'	,'8028949864'	,'8029028812'	,'8029112996'	,'8029172601'	,'8030146547'
--) 
--	^^^^ MassMutual 1.26.2022 repurchase pop ^^^^

--and [Loan Number] in ('7000259042'	,'8004976678'	,'1003345639'	,'8003419138'	,'8005124157'	,'1001819606'	,'8012783955'	,'1005254720'	,'8010904929'	,'8018505482'	,'8019794871'	,'1005121102'	,'8005383655'	,'8025895082'	,'1000414462'	,'1000916794'	,'8017328873'	,'8001478248'	,'8006581763'	,'8009815228'	,'8017491247'	,'8013532739'	,'8018203412'	,'8026011128'	,'8006530783'	,'1002553359'	,'1003369145'	,'8000117810'	,'1005044593'
--)
	--^^^^ MassMutual 07.28.2021 repurchase pop ^^^^