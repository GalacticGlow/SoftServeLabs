The way my batch source was made - only being able to download a full snapshot of the data in its entirety - made me resort to some rather creative and unusual architecture decisions for this lab.
The two helper jobs to the main Lakeflow pipeline were necessary to ensure the streaming table got updated and the batch table was able to read the new gzip snapshots from staging, 
and the two scripts couldnt be included in the main pipeline job as they didnt and couldn't create any new tables during their execution.
The SCD script was written in SQL to allow the Bronze layer to actually have data, as if the script was written in Python it could just read the archive snapshots directly from the staging volume,
which would defeat the purpose of the medallion architecture. The batch bronze table was made as a streaming table for that same reason. 
