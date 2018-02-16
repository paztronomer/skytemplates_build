#! /bin/bash
echo Explicity not include S30 and N30
sky_template -s config/g_skytemplate_y4e1.config -l log/g_skytemplate_y4e1.log -v  -i pca_mini_y4_e1_g_n04.fits -o skytemplate_y4_e1_g_n04.fits -c 3 --input_template g_c03/D{expnum:08d}_g_c{ccd:02d}_r3348p02_pixcor.fits.fz --reject_rms 0.008 --good_filename good_skytemplate_y4_e1_g_n04.fits

# g: 0.008
# r: 0.005
# i: 0.005
# z: 0.003
# Y: 0.004
