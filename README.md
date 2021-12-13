# SQL-PROJECT
/****** Object:  View [IX_Model].[vw_GBI_MD_IX_VENDOR]    Script Date: 12/13/2021 1:53:37 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [IX_Model].[vw_GBI_MD_IX_VENDOR]
AS WITH projection_1 as (SELECT
AFADCD,
AFCEST,
[/BIC/GBSOURCE] AS GBSOURCE,
try_cast("AFCRTD" as date) as Customer_Creation_Date
from [IX_Model].[vw_DIM_GBI_IXMDCM500]),

projection_6 as (SELECT
GBSOURCE,
Q7ADCD,
Q7SSEX,
Q7AB7A
FROM [IX_Model].[vw_GBI_FACT_IXMDCM700] where ("Q7SSEX" in ('0','2','3'))
),

JOIN_1 AS(SELECT
a.AFCEST,
a.AFADCD,
a.GBSOURCE
FROM projection_1 a 
inner join projection_6 b
on a.GBSOURCE = b.GBSOURCE 
and a.AFADCD = b.Q7ADCD
),
 
GBCACM022 as(
SELECT
FCPA_CODE,
LONGDESC 
FROM
[Global_Model].[vw_DIM_GBCACM022]),


IXMDCM900_Projection_1 as(
SELECT
CFA6DT,
CFADCD,
CFALCD,
CFASCD,
CFCEST,
CFK9CD,
[/BIC/GBSOURCE] AS GBSOURCE,
CFCHGT 
FROM
[IX_Model].[vw_DIM_GBI_IXMDCM900]),


IXMDCM900_JOIN_1 AS
(select
a.CFA6DT,
a.CFADCD,
a.CFALCD,
a.CFASCD,
a.CFCEST,
a.CFK9CD,
a.GBSOURCE,
a.CFCHGT,
b.LONGDESC
from IXMDCM900_Projection_1 a
LEFT OUTER JOIN
GBCACM022 b
ON
a.CFALCD = b.FCPA_CODE
),


Projection_3 AS(
select
CFA6DT,
CFADCD,
CFALCD,
CFCEST,
CFK9CD,
GBSOURCE,
CFCHGT,
LONGDESC
from IXMDCM900_JOIN_1
where ("CFK9CD" = 'NONHCPOUE')),



RANK_2 AS (select
CFA6DT,
CFADCD,
CFALCD,
CFCEST,
CFK9CD,
GBSOURCE,
CFCHGT,
LONGDESC
from
(SELECT *,
Row_Number() OVER(PARTITION BY [CFADCD],[CFCHGT] ORDER BY [CFA6DT] DESC) Rank
FROM Projection_3) AS T WHERE RANK = 1),

Rank_4 AS (select
CFA6DT,
CFADCD,
CFALCD,
CFCEST,
CFK9CD,
GBSOURCE,
CFCHGT,
LONGDESC
from
(SELECT *,
Row_Number() OVER(PARTITION BY [CFADCD] ORDER BY [CFCHGT] DESC) Rank
FROM RANK_2) AS T WHERE RANK = 1),


IXMDCM300_Projection_1 AS(
SELECT * FROM [IX_Model].[vw_DIM_GBI_IXMDCM300]
),

GBCACM002 AS(
SELECT 
[4GBCACM002_GBCTRYM],
[4GBCACM002_SOURCE],
[4GBCACM002_S_COUNTRY_CO]
FROM [Global_Model].[vw_DIM_GBCACM002]
),

GBMDCM000 AS(
SELECT
[GB_DESC_LONG] as [GBSOURCE_DESC_LONG],
[GB_GBSOURCE] as [GBSOURCE_DESC_GBSOURCE]
FROM [Global_Model].[vw_DIM_GB_Source_System]
),


IXMDCM300_JOIN AS(
SELECT 
 a.[BCA6DT]
,a.[BCADCD]
,a.[GBSOURCE]
,a.[BCARNA]
,a.[BCASNA]
,a.[BCATNA]
,a.[BCAUNA]
,a.[BCAVNA]
,a.[BCAWNA]
,a.[BCB8NA]
,a.[BCNNNA]
,a.[BCJAST]
,a.[BCA0NA]
,a.[BCJBST]
,a.[BCA1NA]
,a.[BCJCST]
,a.[BCBGNA]
,a.[BCHQCD]
,a.[BCD3NA]
,a.[BCSSA5]
,a.[BCTXD3]
,a.[BCCEEN]
,a.[BCCEDB]
,a.[BCSSJ0]
,a.[BCCEUV]
,a.[BCCEUU]
,a.[BCSSWC]
,a.[BCVIAB]
,a.[BCCEUT]
,a.[BCBGCD]
,a.[BCIDAT]
,a.[BCAPNB]
,a.[BCCEST]
,a.[BCDDNB]
,a.[BCC4NB]
,a.[BCDCNB]
,a.[BCCRTD]
,a.[BCCRTT]
,a.[BCCHGD]
,a.[BCCHGT]
,a.[BCCRTU]
,a.[BCCHGU]
,a.[BCUUA1]
,a.[BCUUMA]
,a.[BCUUCA]
,a.[BCUUD1]
,a.[BCUUSA]
,a.[BCUUQ1]
,a.[BCUU25]
,a.[BCUU40]
,a.[BCACLW]
,a.[BCAF2Q]
,a.[BCAF2V]
,a.[BCAF2W]
,a.[ETL_LOAD_DTE]
,b.[4GBCACM002_GBCTRYM]
,b.[4GBCACM002_S_COUNTRY_CO]
,c.[GBSOURCE_DESC_LONG]

FROM IXMDCM300_Projection_1 a 
LEFT OUTER JOIN GBCACM002 b 
ON a.[BCCEEN] = b.[4GBCACM002_S_COUNTRY_CO]
AND a.[GBSOURCE] = b.[4GBCACM002_SOURCE]

LEFT OUTER JOIN GBMDCM000 c ON
a.[GBSOURCE] = c.[GBSOURCE_DESC_GBSOURCE]
),

IXMDCM300_Rank_1 as (
select * from 
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY [BCADCD] ORDER BY [BCA6DT] DESC) Rank
FROM IXMDCM300_JOIN) AS T WHERE RANK = 1
),


Projection_2 as(
select 
BCTXD3,
[4GBCACM002_GBCTRYM],
BCCHGD,
BCAUNA,
BCATNA,
BCCRTD,
BCSSJ0,
GBSOURCE,
BCA6DT,
BCADCD,
BCB8NA,
BCCEDB,
BCASNA,
BCARNA,
BCDCNB,
BCC4NB
from IXMDCM300_Rank_1
),



RANK_3 as (
select * from
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY [BCADCD] ORDER BY [BCA6DT] DESC) Rank
FROM Projection_2) AS T WHERE RANK = 1
),

Projection_10 as(
select
BCADCD,
GBSOURCE ,
BCCRTD
from IXMDCM300_Rank_1
),
Rank_6 as(
select *
from
(SELECT *,
Row_Number() OVER(PARTITION BY [BCADCD] ORDER BY [BCCRTD] ASC) Rank
FROM Projection_10) AS T WHERE RANK = 1
),


JOIN_2 as (
select
a.AFCEST,
a.AFADCD,
b.BCTXD3,
b.[4GBCACM002_GBCTRYM],
b.BCCHGD,
b.BCAUNA,
b.BCATNA,
b.BCCRTD,
b.BCSSJ0,
b.GBSOURCE,
b.BCA6DT,
b.BCADCD,
b.BCB8NA,
b.BCCEDB,
b.BCASNA,
b.BCARNA,
b.BCDCNB,
b.BCC4NB
FROM JOIN_1 a
INNER JOIN RANK_3 b ON 
a.GBSOURCE = b.GBSOURCE
AND a.AFADCD = b.BCADCD
),




IXFICMAR1_Projection_1 as(
select
B5CYVA,
GBSOURCE,
B5ASCD,
B5AKNB,
B5H9CD,
B5AUNB,
B5CHCE,
B5CICE,
B5CKCE,
B5CBCE,
B5CACE,
count(1) as "1ROWCOUNT"
from
[IX_Model].[vw_FACT_GBI_IXFICMAR1]
group by
B5CYVA,
GBSOURCE,
B5ASCD,
B5AKNB,
B5H9CD,
B5AUNB,
B5CHCE,
B5CICE,
B5CKCE,
B5CBCE,
B5CACE
),

IXFICMAR1_Aggregation as(
SELECT
sum(B5CYVA) as B5CYVA,
GBSOURCE,
B5ASCD,
B5AKNB,
B5H9CD,
B5AUNB,
B5CHCE,
B5CICE,
sum("1ROWCOUNT") as "1ROWCOUNT",
B5CKCE,
B5CBCE,
B5CACE
from IXFICMAR1_Projection_1
GROUP BY

GBSOURCE,
B5ASCD,
B5AKNB,
B5H9CD,
B5AUNB,
B5CHCE,
B5CICE,
B5CKCE,
B5CBCE,
B5CACE
),

Projection_5 as(
select
GBSOURCE,
B5CBCE,
B5CACE
From IXFICMAR1_Aggregation
),



STAR_GBI_FIAP_IX_Projection_1 as(
select
Q7AB7A,
Q7ADCD,
Q7SSEX,
GBSOURCE
FROM [IX_Model].[vw_GBI_FACT_IXMDCM700]
where ("Q7SSEX" in ('0','2','3'))
),



IXMDCM500_Projection_2 as(
select
AFADCD,
AFCEST,
AFCRTD,
[/BIC/GBSOURCE] as GBSOURCE,
try_cast("AFCRTD" AS date) as Customer_Creation_Date
from [IX_Model].[vw_DIM_GBI_IXMDCM500]
),
           
STAR_GBI_FIAP_IX_Projection_2 as(
select
AFADCD,
AFCEST,
GBSOURCE
from IXMDCM500_Projection_2
),




STAR_GBI_FIAP_IX_JOIN_1 as(
select
a.GBSOURCE,
a.Q7ADCD,
a.Q7SSEX,
a.Q7AB7A,
b.AFADCD,
b.AFCEST
FROM
STAR_GBI_FIAP_IX_Projection_1 a
INNER JOIN
STAR_GBI_FIAP_IX_Projection_2 b
ON
a.GBSOURCE = b.GBSOURCE
AND
a.Q7ADCD = b.AFADCD
),


IXFICMAP22_Projection_1 as(
select
B7BPNA,
B7LQCD,
[/BIC/GBSOURCE] as GBSOURCE,
B7ASCD,
B7BYNR,
B7FIDT,
B7CNCE,
B7CACE,
B7AKNB,
B7FJDT
from 
[IX_Model].[vw_FACT_GBI_IXFICMAR2]
),


GBCACM001 as(
select
GBCRM,
SOURCE,
S_CURRENCY_C
from [Global_Model].[vw_GBI_DIM_GBCACM001]

),


IXFICMAP22_JOIN_1 AS(
SELECT
a.B7BPNA,
a.B7LQCD,
a.GBSOURCE,
a.B7ASCD,
a.B7BYNR,
a.B7FIDT,
a.B7CACE,
a.B7AKNB,
a.B7FJDT,
b.GBCRM
FROM
IXFICMAP22_Projection_1 a
LEFT OUTER JOIN
GBCACM001 b
ON
a.GBSOURCE = b.SOURCE
AND
a.B7CNCE = b.S_CURRENCY_C
),

         

IXFICMAP22_Aggregation as(
select *,
1 as dummy,
try_cast("B7FJDT" as date) as Payment_Date
from IXFICMAP22_JOIN_1
),

   

STAR_GBI_FIAP_IX_PROJECTION_3 AS(
SELECT
dummy,
Payment_Date,
B7BPNA,
B7LQCD,
GBSOURCE,
B7ASCD,
B7BYNR,
GBCRM,
B7CACE,
B7AKNB
FROM IXFICMAP22_Aggregation
),


STAR_GBI_FIAP_IX_JOIN_2 AS(
SELECT
a.GBSOURCE,
a.Q7ADCD,
a.Q7SSEX,
a.Q7AB7A,
a.AFADCD,
a.AFCEST,
b.Payment_Date,
b.B7BPNA,
b.B7LQCD,
b.B7ASCD,
b.B7BYNR,
b.GBCRM,
b.B7AKNB,
b.dummy
FROM
STAR_GBI_FIAP_IX_JOIN_1 a
INNER JOIN
STAR_GBI_FIAP_IX_PROJECTION_3 b
on a.GBSOURCE = b.GBSOURCE
and a.AFADCD = b.B7CACE
),

STAR_GBI_FIAP_IX_Projection_4 as(
select
B5CYVA,
GBSOURCE,
B5ASCD,
B5AKNB,
B5H9CD,
B5AUNB,
B5CHCE,
B5CICE,
B5CKCE,
B5CBCE
from IXFICMAR1_Aggregation
),


STAR_GBI_FIAP_IX_JOIN_3 AS(
select
a.GBSOURCE,
a.Q7ADCD,
a.Q7SSEX,
a.Q7AB7A,
a.AFADCD,
a.AFCEST,
a.Payment_Date,
a.B7BPNA,
a.B7LQCD,
a.B7ASCD,
a.B7BYNR,
a.GBCRM,
a.B7AKNB,
a.dummy,
b.B5ASCD,
b.B5CYVA,
b.B5H9CD,
b.B5AUNB,
b.B5CHCE,
b.B5CICE,
b.B5CKCE,
b.B5CBCE
from 
STAR_GBI_FIAP_IX_JOIN_2 a
inner join
STAR_GBI_FIAP_IX_Projection_4 b
on
a.GBSOURCE = b.GBSOURCE AND
a.B7ASCD = b.B5ASCD AND
a.B7AKNB = b.B5AKNB
),


IXFICMAP32_Aggregation as(
select
ARASCD,
[/BIC/GBSOURCE] AS GBSOURCE,
ARALNA,
ARAKNB,
1 AS dummy
from [IX_Model].[vw_FACT_GBI_IXFICMAR3]
),

STAR_GBI_FIAP_IX_Projection_5 as(
select
ARASCD,
GBSOURCE,
ARALNA,
ARAKNB
from IXFICMAP32_Aggregation
),
STAR_GBI_FIAP_IX_Join_4 as(
select
a.GBSOURCE,
a.Q7ADCD,
a.Q7SSEX,
a.Q7AB7A,
a.AFADCD,
a.AFCEST,
a.Payment_Date,
a.B7BPNA,
a.B7LQCD,
a.B7ASCD,
a.B7BYNR,
a.GBCRM,
a.B7AKNB,
a.B5CYVA,
a.B5H9CD,
a.B5AUNB,
a.B5CHCE,
a.B5CICE,
a.B5ASCD,
a.B5CKCE,
a.B5CBCE,
a.dummy,
b.ARALNA
from STAR_GBI_FIAP_IX_Join_3 a
inner join STAR_GBI_FIAP_IX_Projection_5 b ON
a.GBSOURCE = b.GBSOURCE AND
a.B5H9CD = b.ARASCD AND
a.B5AUNB = b.ARAKNB
),

STAR_GBI_FIAP_IX_Projection_6 as(
select
GBSOURCE,
DSASCD,
DSF7CD,
DSAECD,
DSB0ST,
DSBZST,
1 as dummy
from [IX_Model].[vw_FACT_GBI_IXFICMAR4]
),

STAR_GBI_FIAP_IX_Join_5 AS(
SELECT
a.GBSOURCE,
a.Q7ADCD,
a.Q7SSEX,
a.Q7AB7A,
a.AFADCD,
a.AFCEST,
a.Payment_Date,
a.B7BPNA,
a.B7LQCD,
a.B7ASCD,
a.B7BYNR,
a.GBCRM,
a.B7AKNB,
a.B5CYVA,
a.B5H9CD,
a.B5AUNB,
a.B5CHCE,
a.B5CICE,
a.ARALNA,
a.B5CKCE,
a.B5CBCE,
a.dummy
from STAR_GBI_FIAP_IX_Join_4 a
inner join
STAR_GBI_FIAP_IX_Projection_6 b
on
a.GBSOURCE = b.GBSOURCE AND
a.B5ASCD = b.DSASCD AND
a.B5CHCE = b.DSF7CD AND
a.B5CICE = b.DSAECD
),


STAR_GIAB_FIAP_IX_Projection_8 as(
select
GBSOURCE as System_Name,
Q7ADCD,
Q7SSEX,
Q7AB7A,
AFADCD as Vendor_Number,
AFCEST,
Payment_Date,
B7BPNA as Reference_ID,
B7LQCD as Payment_Method_Description,
B7ASCD,
B7BYNR as Transaction_ID,
GBCRM as Payment_Currency,
B7AKNB,
B5CYVA,
B5H9CD,
B5AUNB,
B5CHCE,
B5CICE,
ARALNA as Vendor_Invoice_Number,
B5CBCE,
dummy,
B5CBCE as Business_Unit
FROM STAR_GBI_FIAP_IX_Join_5
),

     

STAR_GBI_FIAP_IX_Aggregation as(
select
System_Name,
Q7ADCD,
Q7SSEX,
Q7AB7A,
Vendor_Number,
AFCEST,
Payment_Date,
Reference_Id,
Payment_Method_Description,
B7ASCD,
Transaction_ID,
Payment_Currency,
B7AKNB,
B5H9CD,
B5AUNB,
B5CHCE,
B5CICE,
Vendor_Invoice_Number,
B5CYVA,
B5CBCE,
Business_Unit,
try_cast(1 as decimal(17,3))as US_Currency_Amount,
case "Payment_Method_Description"
when 'ACHC' then 'E'
when'CHECK' then '2'
when'MANPAY' then 'M'
when'NACHA' then 'E'
when'USCHECK' then '2'
when'USCHECK1' then '2'
when'WIRE' then 'M'
when'WIREPAY' then 'M'
else('') end AS Payment_Method_Code,
sum(dummy) as dummy

from STAR_GIAB_FIAP_IX_Projection_8

GROUP BY
System_Name,
Q7ADCD,
Q7SSEX,
Q7AB7A,
Vendor_Number,
AFCEST,
Payment_Date,
Reference_Id,
Payment_Method_Description,
B7ASCD,
Transaction_ID,
Payment_Currency,
B7AKNB,
B5H9CD,
B5AUNB,
B5CHCE,
B5CICE,
Vendor_Invoice_Number,
B5CYVA,
B5CBCE,
Business_Unit
), 

Projection_11 as(
select
Payment_Date,
Vendor_Number,
System_Name
from 
STAR_GBI_FIAP_IX_Aggregation
),


Rank_7 as(
select *
from
(SELECT *,
Row_Number() OVER(PARTITION BY [Vendor_Number] ORDER BY [Payment_Date] ASC) Rank
FROM Projection_11) AS T WHERE RANK = 1
),



Projection_4 AS(
SELECT "GBSOURCE", STRING_AGG("B9BHNA",',') AS B9BHNA,"B9ADCD" AS B9ADCD, 1 AS COUNTER
FROM [IX_Model].[vw_DIM_GBI_IXMDCM600]
GROUP BY "B9ADCD","GBSOURCE"
),

Projection_8 as(
select
GBSOURCE,
GBVENDOR,
POSUS
FROM
[IX_Model].[vw_DIM_GBI_IXMDCM005]
),

Projection_9 as(
select 
GBSOURCE,
CSADCD,
CSAHST,
CSCEST,
CSAECD
FROM 
[IX_Model].[vw_DIM_GBI_IXMDCM113]),

Rank_5 as(
select
GBSOURCE,
CSADCD,
CSCEST,
CSAECD
from
(SELECT *,
Row_Number() OVER(PARTITION BY [CSADCD] ORDER BY [CSCEST] ASC) Rank
FROM Projection_9) AS T WHERE RANK = 1
),

Join_main as(
select
a.AFCEST,
a.AFADCD,
a.BCTXD3,
a.[4GBCACM002_GBCTRYM],
a.BCCHGD,
a.BCAUNA,
a.BCATNA,
a.BCSSJ0,
a.GBSOURCE,
a.BCA6DT,
a.BCADCD,
a.BCB8NA,
a.BCCEDB,
a.BCASNA,
a.BCARNA,
a.BCDCNB,
a.BCC4NB,

b.LONGDESC,
b.CFALCD,

c.B9BHNA,

d.POSUS,

e.Payment_Date,
Case When(d."POSUS" IS NULL or d."POSUS" = '') then '0' when (d."POSUS" = 'S') then 'I' else '0' end as Vendor_Status1,

f.B5CBCE,

g.CSCEST,

h.BCCRTD


FROM Join_2 a left outer join
Rank_4 b on
a.AFADCD = b.CFADCD AND
a.GBSOURCE = b.GBSOURCE

left outer join Projection_4 c on
a.AFADCD = c.B9ADCD 
AND
a.GBSOURCE = b.GBSOURCE

left outer join Projection_8 d on
a.AFADCD = d.GBVENDOR 
AND
a.GBSOURCE = d.GBSOURCE

left outer join Rank_7 e on
a.AFADCD = e.Vendor_Number 
AND
a.GBSOURCE = e.System_Name

left outer join Projection_5 f on
a.AFADCD = f.B5CACE 
AND
a.GBSOURCE = f.GBSOURCE

left outer join Rank_5 g on
a.GBSOURCE = g.GBSOURCE
AND
a.BCADCD = g.CSADCD

left outer join Rank_6 h on
a.AFADCD = h.BCADCD
AND
a.GBSOURCE = h.GBSOURCE
),

Aggregation as(
select
AFCEST,
AFADCD,
POSUS,
B9BHNA AS Vendor_Contact_Name,
BCTXD3 AS Vendor_City,
[4GBCACM002_GBCTRYM] AS Vendor_Country_Code,
BCCHGD AS Last_Update_Date,
BCAUNA,
BCATNA,
BCSSJ0,
GBSOURCE AS System_Name,
BCA6DT,
BCADCD AS Vendor_Number,
BCB8NA AS Vendor_Postal_Code,
BCCEDB AS Vendor_State,
BCASNA,
BCARNA AS Vendor_Name,
BCDCNB,
BCC4NB,
Vendor_Status1,
B5CBCE AS Business_Unit,
LONGDESC AS FCPA_Classification_Description,
CFALCD AS FCPA_Classification,
CSCEST,
BCCRTD,
Payment_Date as Last_Activity_Date,
case BCSSJ0
WHEN '2' THEN BCASNA
WHEN '3' THEN BCASNA+' '+ BCATNA
WHEN '4' THEN BCASNA+' '+ BCATNA+' '+ BCAUNA
WHEN '5' THEN BCASNA+' '+BCATNA+' '+BCAUNA+' ' +BCASNA+' '+BCATNA+' '+BCAUNA
END AS Vendor_Street_Address
--,case when (case when("Vendor_Status1" = '0' when ("AFCEST" = '1' AND "CSCEST" = '1' then 'A' else 'I') then "Vendor_Status1") is null then 'I' when ("Vendor_Status1" = '0' when ("AFCEST" = '1' AND "CSCEST" = '1' then 'A' else 'I') then "Vendor_Status1")) AS Vendor_Status_Code
--,case when (case when("Vendor_Status1" = '0' when ("AFCEST" = '1' AND "CSCEST" = '1' then 'A' else 'I') then "Vendor_Status1") is null then 'I' when ("Vendor_Status1" = '0' when ("AFCEST" = '1' AND "CSCEST" = '1' then 'A' else 'I') then "Vendor_Status1")end) end AS Vendor_Status_Code
,case when (case when("Vendor_Status1" = '0') then (case when ("AFCEST" = '1' AND "CSCEST" = '1') then 'A' else 'I' end) else "Vendor_Status1" end) is null then 'I' else case when ("Vendor_Status1" = '0') then (case when "AFCEST" = '1' AND "CSCEST" = '1' then 'A' else 'I' end ) else "Vendor_Status1" end end AS Vendor_Status_Code
,"BCCRTD" as Vendor_Creation_Date
from join_main
)

select

Vendor_Street_Address,
Vendor_Status_Code,
Last_Activity_Date,
Vendor_City,
Vendor_Country_Code,
Last_Update_Date,
System_Name,
Vendor_Number,
Vendor_Name,
Vendor_Postal_Code,
Vendor_State,
Vendor_Creation_Date,
FCPA_Classification_Description,
FCPA_Classification,
Vendor_Contact_Name,
Business_Unit

from Aggregation;
GO


