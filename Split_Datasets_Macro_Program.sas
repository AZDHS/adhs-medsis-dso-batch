
/* ---------------------------------------------------------------------------------------------------------------------------------------
   Program:      Split Dataset into Smaller Datasets with Max Row Requirement
   Updated:      9-23-2021
   Description:  DSO batch files for MEDSIS cannot have more than 300 rows per file.
                 This program can be used to split one SAS dataset into multiple smaller datasets with only 300 rows per dataset and output
		 them as .txt files for MEDSIS batching.
   --------------------------------------------------------------------------------------------------------------------------------------- */

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
