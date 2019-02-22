#!/bin/bash
nm=Y5dev+12
rms=(0.008 0.009 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08)
for k in ${rms[@]}
    do 
    
    echo RMS=$k
    echo aux naming "${k/./p}"
    
    sky_pca -i bleedmask_binned_fp_u.csv -o pca_"$nm"_u_"${k/./p}"_n04.fits -n 4 --reject_rms $k -s config/u_skypca_"${k/./p}".config -l log/u_skypca_"${k/./p}".log -v

    done

# sky_pca -i bleedmask_binned_fp_u.csv -o pca_mini_2017t2019_Y2stack_u_n04.fits -n 4 --reject_rms 0.08 -s config/u_skypca_2017t2019.config -l log/u_skypca_2017t2019.log -v
