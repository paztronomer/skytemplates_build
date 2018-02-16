#! /bin/bash
echo g-band
sky_pca -i g_bleedmask_binned_fp_y4e1.csv -o pca_mini_y4_e1_g_n04.fits -n 4 --reject_rms 0.008 -s config/g_skypca_y4e1.config -l log/g_skypca_y4e1.log -v
echo Ended, $(date)
