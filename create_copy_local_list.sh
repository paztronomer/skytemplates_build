#!/bin/bash

# Create local bash files for transfer binned_fp files and also create the 
# input tables for sky_pca

bands=(g r i z Y)
for k in ${bands[@]}
    do 
    echo $k band
    # k is set as variable inside awk by -v k=$k
    # we avoid the first line (header) by using FNR > 1
    echo Creating bash copy script 
    awk -F "," -v k=$k 'FNR > 1 {print("cp /archive_data/desarchive/" $1 "/" $2 " binned_fp/" k )}' 'bleedmask_'$k'.csv' > 'copy_'$k'.sh'

    echo Creating paths for sky_pca
    awk -F "," -v k=$k 'FNR > 1 {print("binned_fp/" k "/" $2)}' 'bleedmask_'$k'.csv' > 'local_bleedmask_binned_'$k'.csv'

    done
