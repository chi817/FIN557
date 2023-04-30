/*1 Download the data*/

/*Create a library */
libname project "/home/u63085035/fin557/project";

/* Part 1*/
proc contents data=project.data;
run;

proc print data=project.data;
run;

data data;
set project.data;
run;

/*Calculate the total bankruptcy count for each year 2017-2023*/
proc sql;
select YEAR(BANK_BEGIN_DATE) as YEAR_bankrupted, 
       count(COMPANY_FKEY) as bankruptcy_count_year
from data
where LOC_STATE_COUNTRY='USA'
group by calculated YEAR_bankrupted;
quit;

/*Calculate the bankruptcy count for each SIC each year 2017-2023*/
proc sql;
create table SIC_rank as
select substr(SIC_CODE_FKEY,1,3) as SIC, 
       YEAR(BANK_BEGIN_DATE) as YEAR_bankrupted, 
       count(COMPANY_FKEY) as bankruptcy_count_year,
       SIC_CODE_DESCRIP
from data
where LOC_STATE_COUNTRY='USA'
group by calculated SIC, calculated YEAR_bankrupted
order by calculated YEAR_bankrupted, bankruptcy_count_year desc;
quit;

/*Print only the most bankrupcy observations for every year*/
data SIC;
set SIC_rank;
by YEAR_bankrupted;
if first.YEAR_bankrupted;
run;

proc print data=SIC;
run;

/*Calculate the bankruptcy count for each State each year 2017-2023*/
proc sql;
select LOC_STATE as State, 
       YEAR(BANK_BEGIN_DATE) as YEAR_bankrupted, 
       count(COMPANY_FKEY) as bankruptcy_count_state
from data
where LOC_STATE_COUNTRY='USA'
group by State, calculated YEAR_bankrupted
order by calculated YEAR_bankrupted, bankruptcy_count_state desc;
quit;

/*Join the two tables*/
/*Create year_bank tabke*/
proc sql;
create table year_bank as
select YEAR(BANK_BEGIN_DATE) as YEAR_bankrupted, 
       count(COMPANY_FKEY) as bankruptcy_count_year
from data
where LOC_STATE_COUNTRY='USA'
group by calculated YEAR_bankrupted;
quit;

/*Create state_bank table*/
proc sql;
create table state_bank as
select LOC_STATE as State, YEAR(BANK_BEGIN_DATE) as YEAR_bankrupted, 
       count(COMPANY_FKEY) as bankruptcy_count_state
from data
where LOC_STATE_COUNTRY='USA'
group by State, calculated YEAR_bankrupted
order by calculated YEAR_bankrupted, bankruptcy_count_state desc;
quit;

/*Join*/
proc sql;
create table state as
select s.State, y.YEAR_bankrupted, s.bankruptcy_count_state, y.bankruptcy_count_year, 
       s.bankruptcy_count_state/y.bankruptcy_count_year as bankruptcy_pct format=PERCENT8.2
from year_bank y
full join state_bank s
on y.YEAR_bankrupted=s.YEAR_bankrupted;
quit;

/*Print only the most bankrupcy observations for every year*/
data State_rank;
set State;
by YEAR_bankrupted;
if first.YEAR_bankrupted;
run;

proc print data=State_rank;
run;






/* Part 2*/
proc freq data=data2 order=freq noprint;
     tables FYEAR*STATE /nocum nopercent out=firms;
run;

proc sql;
 create table data4 as 
 select YEAR(BANK_BEGIN_DATE) as YEAR_bankrupted, LOC_STATE, BEST_EDGAR_TICKER
 from data
 where LOC_STATE_COUNTRY='USA';
quit;


proc freq data=data4 order=freq noprint;
     tables YEAR_bankrupted*LOC_STATE /nocum nopercent out=bankrupt;
run;

 

proc sql;
  select COUNT as BANKRUPT, LOC_STATE as STATE, YEAR_bankrupted
  from bankrupt;
quit;

 

/* Merge bankrup and firm datasets */ 
proc sql;
  create table freqtable as 
  select f.STATE, f.FYEAR, f.count, b.COUNT as BANKRUPT, round(b.COUNT/f.count,0.001)*100 as Ratio_firm_bankrupt_pct
  from bankrupt b
  left join firms f
  on b.YEAR_bankrupted = f.FYEAR AND b.LOC_STATE = f.STATE;
quit;
 
 
proc sql; 
select STATE, FYEAR, BANKRUPT, Ratio_firm_bankrupt_pct
from freqtable
where FYEAR = 2020
order by ratio_firm_bankrupt_pct desc;
run;





/*Part 3*/
proc contents data=project.data2;
run;

proc print data=project.data2 (obs=20);
run;

data data2;
set project.data2;
run;

/* Join data and data2 as merge*/
proc sql;
create table merge as
select *, YEAR(d.BANK_BEGIN_DATE) as YEAR_bankrupted
from data d left join
     data2 d2
on d.BEST_EDGAR_TICKER=d2.TIC
where d2.TIC is not null and 
      d.BEST_EDGAR_TICKER is not null;
quit;

proc print data=merge;
run;


/* Import data3*/
proc contents data=project.data3;
run;

data data3;
set project.data3;
run;

/* Create market table to calculate market_value*/
proc sql;
create table market as
select YEAR(DATE) as YEAR, COMNAM, TICKER,
       mean(PRC) as average_stock_price,
       mean(SHROUT) as average_shrout,
       mean(PRC)* mean(SHROUT) as market_value
from data3
where COMNAM is not null and
      TICKER is not null and 
      PRC is not null and 
      SHROUT is not null
group by TICKER, YEAR;
quit;

/* Remove duplicate rows*/
proc sort data=market out=market2 nodupkey;
by _all_;
run;

proc print data=market2 (obs=50);
run;

/* Join merge and market table as merge2*/
proc sql;
create table merge2 as
select m.YEAR_bankrupted, m2.*, 
       m.EBIT, m.AT,
       m.SALE, m.LT, 
       m.WCAP, m.RE
from market2 m2 left join
     merge m
     on m2.YEAR=m.FYEAR and
     m2.TICKER=m.TIC
where m.FYEAR is not null and
      m.TIC is not null and
      m.EBIT is not null and
      m.AT is not null and
      m.SALE is not null and
      m.LT is not null and
      m.WCAP is not null and
      m.RE is not null;
quit;

proc print data=merge2 (obs=50);
run;


/* Create Variables for Z-score */ 
proc sql; 
create table zscore as 
select *, 
       EBIT/AT as A, 
       SALE/AT as B, 
       market_value/LT/1000 as C,
       WCAP/LT as D,
       RE/AT as E
from merge2;
quit;

/* Calculate the Z-score for each company that got bankrupted*/
proc sql;
create table zscore2 as 
select YEAR, TICKER, YEAR_bankrupted, COMNAM, A, B, C, D, E, 
       3.3*A+0.99*B+0.6*C+1.2*D+1.4*E as z_score,
       3.3*A+0.99*B+0.6*C+1.2*D+1.4*E as z_score_category
from zscore
order by TICKER, YEAR;
quit;

/* format Z-score with according Interpretation */ 
proc format;
    value zscorefmt  low-<1.8   = 'Probability of Financial distress is very high'
                     1.8-<2.7   = 'Good chances of going bankrupt within 2 years'
				     2.7-<2.99  = 'On Alert'
                     3-high     = 'Safe';
run;

proc print data=zscore2;
    var YEAR TICKER YEAR_bankrupted COMNAM z_score z_score_category;
    format  z_score_category zscorefmt.;
run;

proc sql;
create table zscore3 as
select YEAR, TICKER, YEAR_bankrupted, COMNAM, z_score, 
       z_score_category format zscorefmt.
from zscore2
group by TICKER
having YEAR=YEAR_bankrupted;
quit;

proc print data=zscore3;
run;

data zscore4;
	set zscore3;
	drop z_score_category;
run;

proc print data=zscore4;
run;

data high_Risk on_alert safe;
	set zscore4;
	length z_score_category $50;
	if z_score <1.8  then do;
	z_score_category = 'Probability of Financial distress is very high';
		output high_Risk;
	end;
	else if z_score <2.99  then do;
	z_score_category = 'On Alert';
		 output on_alert;
	end;
    else do;
    z_score_category = 'safe';
		 output safe;
	end;
run;

proc print data=high_Risk;
run;

proc print data=on_alert;
run;

proc print data=safe;
run;






