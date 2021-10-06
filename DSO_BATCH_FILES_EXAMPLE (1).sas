
/* -----------------------------------------------------------------------------------------------------------------------
   Program:      DSO Batch Code Example
   Written:      7/16/2021
   Author:       Cymone Gates, MPH 


   Description:  This program was created to provide an example of code used to create DSO batch files for MEDSIS. It was 
                 pulled from the QUALTRICS_DSO_BATCH program and has been simplied for this purpose.
---------------------------------------------------------------------------------------------------------------------------- */

*bringing in COVID-19 DSO variable list (code.dsobatchvarlist) in the desired order. Desired order is needed if there are parent-child questions i.e. parent questions should come before child;
*Cymone created this herself based on the MEDSIS data dictionary;
LIBNAME code '\\groups\edc\Groups\EPI\MEDSIS\Surveillance Reports\COVID\Qualtrics\SAS Codes\DSO Batch';

*creating macro variable for list of DSO variables in desired order;
PROC SQL; SELECT FIELD INTO: VARLIST SEPARATED BY ' ' FROM code.dsobatchvarlist;QUIT;

*creating dataset for DSO variables;
DATA DSO 
/*dropping old numeric variables*/
(DROP=HOSPDT HOSPDUR VAXDT1INV VAXDT2INV PTWORKDT: HHNUM NONHHNUM EVENTNUM
/*renaming new variables to match MEDSIS names*/
RENAME=(SXCVDTOES=SXTCOVTOES HOSPDT_TXT=HOSPDT HOSPDUR_TXT=HOSPDUR VAXDT1INV_TXT=VAXDT1INV VAXDT2INV_TXT=VAXDT2INV
        PTWORKDT1_TXT=PTWORKDT1 PTWORKDT2_TXT=PTWORKDT2 PTWORKDT3_TXT=PTWORKDT3 HHNUM_TXT = HHNUM NONHHNUM_TXT=NONHHNUM EVENTNUM_TXT=EVENTNUM));
	
SET COVIDRECODE; *dataset of survey data that's been reformatted to match MEDSIS response values e.g. Qualtrics had value of 0 but MEDSIS needed value of "No";

*creating character verisions of the numeric variables for DSO batching. If you leave them numeric, you'll get errors during batching;
HOSPDT_TXT    =PUT(HOSPDT, mmddyyd10.);
HOSPDUR_TXT   =PUT(HOSPDUR, 8.);
VAXDT1INV_TXT =PUT(VAXDT1INV, mmddyyd10.);
VAXDT2INV_TXT =PUT(VAXDT2INV, mmddyyd10.);
PTWORKDT1_TXT=PUT(PTWORKDT1, mmddyyd10.);
PTWORKDT2_TXT=PUT(PTWORKDT2, mmddyyd10.);
PTWORKDT3_TXT=PUT(PTWORKDT3, mmddyyd10.);
HHNUM_TXT    =PUT(HHNUM, 8.);
NONHHNUM_TXT =PUT(NONHHNUM, 8.);
EVENTNUM_TXT =PUT(EVENTNUM,8.);

*remove any line feeds;
array m _CHARACTER_; do i=1 to dim(m); if m[i]^="" then m[i]=translate(m[i],' ','0A'x); end; drop i; 

RUN; 

*re-ordering DSO columns to work for parent-child questions;
DATA DSO; RETAIN &VARLIST; SET WORK.DSO;RUN;

*sorting for proc transpose;
proc sort data=DSO; by MEDSISID PTSYMPT;run;


*proc transpose to reformat the dataset for import into MEDSIS;
*MEDSIS requires a long format for DSO field imports instead of the standard wide format;
proc transpose data=DSO out=long1 NAME=QCODE;
   by MEDSISID;
   VAR PTSYMPT--VAXDT2INV;
run;


OPTIONS MISSING = "";

*this macro variable is optional. It was used in my program because I needed to differentiate survey data;
*if you are only using data from one survey, you don't need it but if you remove it here make sure 
 	you remove it from all instances in this code;
%let SURVEY=QUALTRICS;

*final DSO dataset;
*had to recode some variables due to data transformation after transposing;
data finalnomiss_&SURVEY (DROP=COL1);
format MEDSISID QCODE CODEVALUE TEXT_VALUE; 
set long1;
LENGTH TEXT_VALUE $300;

*removing data with only periods from date to char conversion - all others will have their DSO response moved to the TEXT_VALUE variable;
IF STRIP(COL1)="." THEN TEXT_VALUE=""; ELSE TEXT_VALUE=COL1;

*certain DSO fields require both a CODEVALUE AND TEXT_VALUE. I have to explicity list them here. The MEDSIS team provides the information
on which fields need both CODEVALUE and TEXT_VALUE and which do not;
IF QCODE in('PTSYMPT','FEVER','CGH','SRTHRT','SOB','DIFBRTH','SXCHILLS','SXRIGOR','SXHDACHE','SXMSCLACHE','SXNAUSEA','SXVOMIT','SXDIARRHEA','SXCOVRASH','SXBRAINFOG','SXCHESTPAIN','SXDIZZY','SXLOSSAPPETITE',
	'SXCOLDSX','SXFATIGUE','SXTASTE','SXTCOVTOES','SXOTHER','HOSPITL','HOSPICU','INTUBTD','ECMO','PNEU','ARDS','COMORBDIABETES','COMORBCARDDZ',
	'COMORBHYPERTEN','COMORBASTH','COMORBCHRPULMDZ','COMORBCHRKIDNYDZ','COMORBDIAL','COMORBCHRLIVRDZ','COMORBOBESITY','COMORBCANC','COMORBIMMUN',
	'COMORBMED','COMORBALLERG','COMORBOTHER','COMORBSMOKE','LIVSITCNGSET','CONTACTCASE','EVENT','EPITRAVEL','HIRISKOCC','SXCOVRASH','SXBRAINFOG','SXDIZZY','SXLOSSAPPETITE','SXCHESTPAIN','PTWORKNUM','PTWORKTYPE1',
    'PTWORKATT1','PTWORKTYPE2','PTWORKATT2','PTWORKTYPE3','PTWORKATT3','HIRISKOCC','EVENTNONHH','EVENTNUM','EVENT1TYPE','EVENT1SIZE','EVENT1SETTING','EVENT2TYPE','EVENT2SIZE','EVENT2SETTING','EVENT3TYPE','EVENT3SIZE',
    'EVENT3SETTING','EVENT4TYPE','EVENT4SIZE','EVENT4SETTING','EVENT5TYPE','EVENT5SIZE','EVENT5SETTING','EVENT6TYPE','EVENT6SIZE','EVENT6SETTING','EVENT7TYPE','EVENT7SIZE','EVENT7SETTING','EVENT8TYPE','EVENT8SIZE',
	'EVENT8SETTING','EVENT9TYPE','EVENT9SIZE','EVENT9SETTING','EVENT10TYPE','EVENT10SIZE','EVENT10SETTING','VAXINV','VAXMANINV','VAXNUMINV') 
then CODEVALUE=TEXT_VALUE;
ELSE CODEVALUE=""; *all other fields do not need a CODEVALUE so I can leave it blank;

*removing spaces from numeric DSO variables to prevent batching errors;
IF QCODE IN ('HHNUM','NONHHNUM','PTWORKNUM','EVENTNUM','HOSPDUR') THEN DO;
	CODEVALUE  = COMPRESS(CODEVALUE);
	TEXT_VALUE = COMPRESS(TEXT_VALUE);
END;

*creating a DSO dataset that only batches variables that have data i.e. no missing values to avoid DSO batch errors;
if CODEVALUE = "" AND TEXT_VALUE = "" THEN DELETE; 

RUN;

*where DSO files will be output;
%let outpath = \\groups\edc\Groups\EPI\Routing\Electronic Files\2021\COVID DSO UPDATES;


/*This macro program splits and exports the DSO batch dataset into equal txt files, where the number of rows is less than or equal to 300*/
/*MEDSIS's DSO Batch feature doesn't accept more than 300 rows of data at a time*/

%macro exportagain;

*macro variable to check if there's data to batch;
%LET C = 0 ; data _null_ ;   SET  finalnomiss_&SURVEY   nobs=NumObs ;   CALL SYMPUT("C", LEFT(NumObs)) ;  stop ; run ;

*if there's data to batch, the macro program will generate txt files with 300 rows per file; 
%IF &C > 0 %THEN %DO;
data _NULL_;
	if 0 then set finalnomiss_&SURVEY nobs=n;
  call symputx('nrows',n);
  stop;
run;
%put &nrows;

%let nsets = %sysevalf((&nrows./300),ceil);
%put &nsets;

%let max = %sysevalf(&nsets.*300);
%put &max;




%macro split (ndsn=&nsets.);

data %do i=1 %to &ndsn.; finalnomiss_&SURVEY&i. %end; ;
retain x;
set finalnomiss_&SURVEY nobs=nobs;

if _n_ = 1 then do;
	if mod(nobs,&ndsn.) = 0 then x=int(nobs/&ndsn.);
	else x=int(nobs/&ndsn.)+1;
end;

if _n_ <= x then output finalnomiss_&SURVEY.1;
%do i=2 %to &ndsn.;
	else if _n_ <= (&i.*x) then output finalnomiss_&SURVEY&i.;
%end;

run;
%mend split;

%split;

*output txt files to outpath;
%macro export;

%do i=1 %to &nsets.;


proc export data=finalnomiss_&SURVEY&i. (drop= x)
outfile="&outpath\NCV_&SURVEY._DSO_&file. - &i..txt"
dbms=tab replace;
run;
%check_for_errors
%end;

%mend export;

%export; *run macro program;

%END;
%mend;
%exportagain