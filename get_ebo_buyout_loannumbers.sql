-- get ebo loan numbers
--SERVER: SRV-SQLFC-1
--101793 loans from EBO SPD table
--96,004 loans from PBOSale

--SELECT DISTINCT cast(a.LoanID as bigint) as LoanId from SPD.ServInvestment.EBO_Pools (nolock) as A
select distinct LoanID as LoanId from spd.ServInvestment.PBOSale (nolock) as A