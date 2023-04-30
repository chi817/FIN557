/*1 Download the data*/

/*Create a library */
libname project "/home/u63163426/sasuser.v94/fin557/project";

proc contents data=project.data;
run;

proc print data=project.data;
run;

data data;
set project.data;
run;

/*Calculate the total bankruptcy count for each year 2017-2023*/
proc sql;
select YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_year
from data
where LOC_STATE_COUNTRY='USA'
group by calculated YEAR;
quit;

/*Calculate the bankruptcy count for each SIC each year 2017-2023*/
proc sql;
create table SIC_rank as
select substr(SIC_CODE_FKEY,1,3) as SIC, 
       YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_year
from data
where LOC_STATE_COUNTRY='USA'
group by calculated SIC, calculated YEAR
order by calculated YEAR, bankruptcy_count_year desc;
quit;

data SIC;
set SIC_rank;
by YEAR;
if first.YEAR;
run;

proc print data=SIC;
run;

/*Calculate the bankruptcy count for each State each year 2017-2023*/
proc sql;
select LOC_STATE as State, 
       YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_state
from data
where LOC_STATE_COUNTRY='USA'
group by State, calculated YEAR
order by calculated YEAR, bankruptcy_count_state desc;
quit;



/*Join the two tables*/
proc sql;
create table year_bank as
select YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_year
from data
where LOC_STATE_COUNTRY='USA'
group by calculated YEAR;
quit;

proc sql;
create table state_bank as
select LOC_STATE as State, YEAR(BANK_BEGIN_DATE) as YEAR, 
       count(COMPANY_FKEY) as bankruptcy_count_state
from data
where LOC_STATE_COUNTRY='USA'
group by State, calculated YEAR
order by calculated YEAR, bankruptcy_count_state desc;
quit;

proc sql;
create table state as
select s.State, y.YEAR, s.bankruptcy_count_state, y.bankruptcy_count_year, 
       s.bankruptcy_count_state/y.bankruptcy_count_year as bankruptcy_pct format=PERCENT8.2
from year_bank y
full join state_bank s
on y.YEAR=s.YEAR;
quit;

data State_rank;
set State;
by YEAR;
if first.YEAR;
run;

proc print data=State_rank;
run;




proc contents data=project.data2;
run;

proc print data=project.data2 (obs=20);
run;

data data2;
set project.data2;
run;

proc sql;
select distinct TIC
from data2;
quit;

proc sql;
create table merge as
select *
from data d left join
     data2 d2
on d.BEST_EDGAR_TICKER=d2.TIC
where d2.TIC is not null and 
      d.BEST_EDGAR_TICKER is not null;
quit;

proc print data=merge;
run;


/* Manipulate data string */
proc contents data=project.data3;
run;

data data3;
set project.data3;
run;

proc sql outobs=20;
create table market as
select YEAR(DATE) as YEAR, COMNAM, TICKER,
       mean(PRC) as average_stock_price,
       mean(SHROUT) as average_shrout,
       mean(PRC)* mean(SHROUT) as market_value
from data3
where COMNAM is not null
group by COMNAM, calculated YEAR;
quit;

proc print data=market (obs=10);
run;



/* Create Variables for Z-score */ 
proc sql outobs=10; 
 create table zscore as 
 select data2.FYEAR, data2.TIC, 
        data2.EBIT/data2.AT as A, 
        data2.SALE/data2.AT as B, 
        m2.market_value/m1.LT as C,
        data2.WCAP/data2.LT as D,
        data2.RE/data2.AT as E
 from data2, merge m1, market m2
 group by data2.FYEAR;
quit;

proc sql outobs=10;
select FYEAR, TIC, A, B, C, D, E, 3.3*A+0.99*B+0.6*C+1.2*D+1.4*E as z_score
from zscore;
quit;








