
/* -----------------------------------------------------------------------------------------------------------------------
   Program:      DSO Batch Code Example
   Written:      7/16/2021
   Author:       Cymone Gates, MPH 
   Description:  This program was created to provide an example of code used to create DSO batch files for MEDSIS. It was 
                 pulled from the QUALTRICS_DSO_BATCH program and has been simplied for this example.
---------------------------------------------------------------------------------------------------------------------------- */
/*I want to make a change*/


*creating dataset for DSO variables;
DATA DSO 
	/*dropping old numeric variables*/
	(DROP=HOSPDT HOSPDUR VAXDT1INV VAXDT2INV PTWORKDT: HHNUM NONHHNUM EVENTNUM

	/*renaming new variables to match MEDSIS names*/
	RENAME=(HOSPDT_TXT=HOSPDT HOSPDUR_TXT=HOSPDUR VAXDT1INV_TXT=VAXDT1INV VAXDT2INV_TXT=VAXDT2INV
        	PTWORKDT1_TXT=PTWORKDT1 PTWORKDT2_TXT=PTWORKDT2 PTWORKDT3_TXT=PTWORKDT3 HHNUM_TXT = HHNUM NONHHNUM_TXT=NONHHNUM EVENTNUM_TXT=EVENTNUM
		)
	);
	
SET COVIDRECODE; *dataset of survey data that's been reformatted to match MEDSIS response values e.g. Qualtrics had value of 0 but MEDSIS needed value of "No";

*creating character verisions of the numeric variables for DSO batching. If you leave them numeric, you'll get errors during batching;
HOSPDT_TXT    =PUT(HOSPDT,    mmddyyd10.);
VAXDT1INV_TXT =PUT(VAXDT1INV, mmddyyd10.);
VAXDT2INV_TXT =PUT(VAXDT2INV, mmddyyd10.);
PTWORKDT1_TXT =PUT(PTWORKDT1, mmddyyd10.);
PTWORKDT2_TXT =PUT(PTWORKDT2, mmddyyd10.);
PTWORKDT3_TXT =PUT(PTWORKDT3, mmddyyd10.);
HOSPDUR_TXT   =PUT(HOSPDUR, 8.);
HHNUM_TXT     =PUT(HHNUM, 8.);
NONHHNUM_TXT  =PUT(NONHHNUM, 8.);
EVENTNUM_TXT  =PUT(EVENTNUM,8.);


RUN; 

*bringing in COVID-19 DSO variable list (code.dsobatchvarlist) in the desired order.;
*Desired order is needed if there are parent-child questions i.e. parent questions should come before child;
*Cymone created this herself based on the MEDSIS data dictionary;

LIBNAME code '...\DSO Batch';

*creating macro variable that lists the DSO variables in desired order;
PROC SQL; SELECT FIELD INTO: VARLIST SEPARATED BY ' ' FROM code.dsobatchvarlist;QUIT;

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

*final DSO dataset;
*had to recode some variables due to data changes after transposing;

data finalnomiss (DROP=COL1);

format MEDSISID QCODE CODEVALUE TEXT_VALUE; 

set long1;

LENGTH TEXT_VALUE $300;

*removing data with periods from date to char conversion - all others will have their DSO response moved to the TEXT_VALUE variable;
IF STRIP(COL1)="." THEN TEXT_VALUE=""; ELSE TEXT_VALUE=COL1;

/*certain DSO fields require both a CODEVALUE AND TEXT_VALUE so explicity list them here. 
 The MEDSIS team can provide the information on which fields need both and which do not*/

IF QCODE in('')  then CODEVALUE=TEXT_VALUE;  else CODEVALUE=""; 

*removing spaces from numeric DSO variables to prevent batching errors;
IF QCODE IN ('HHNUM','NONHHNUM','PTWORKNUM','EVENTNUM','HOSPDUR') THEN DO;
	CODEVALUE  = COMPRESS(CODEVALUE);
	TEXT_VALUE = COMPRESS(TEXT_VALUE);
END;

*creating a DSO dataset that only batches variables that have data i.e. no missing values to avoid DSO batch errors;
if CODEVALUE = "" AND TEXT_VALUE = "" THEN DELETE; 

RUN;

%let outpath = ; *<----------------Enter the file path for exporting;

%let dataset = ;*<-----------------Enter the name of the SAS dataset that needs to be split;

*create macro var with the number of rows in the dataset;
PROC SQL NOPRINT;SELECT COUNT(*) INTO: nrows  trimmed FROM &dataset;QUIT; %put &nrows;

%macro splitnexport;

*proceed if there is data in your dataset;
%if &nrows > 0 %then %do;

	*max # of rows you want per split dataset;
	%let nsize = ; *<----------------Enter the maximum # of rows per dataset;

	*# of datasets that will be created based on the &nsize and &nrows values;
	%let nsets = %sysevalf((&nrows./&nsize),ceil); %put &nsets;

	/*Splits and exports &dataset into new datasets, where the number of rows in the split datasets are less than or equal to &nsize*/
	%macro split;

		data %do i=1 %to &nsets.; &dataset&i. %end;;
		set &dataset;

		if _n_ <= &nsize then output &dataset.1;
			%do i=1 %to &nsets.;
				else if _n_ <= (&i.*&nsize) then output &dataset&i.;
			%end;

		run;

	*output txt files to outpath - You can comment this out if you do not need to export the datasets;
		%do i=1 %to &nsets.;

		proc export data=&dataset&i. outfile="&outpath\&dataset. - &i..txt" dbms=tab replace;
        run;

        %end;

	%mend;

	%split *execute split macro;

%end;
%mend;


%splitnexport *execute macro;
