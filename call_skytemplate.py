''' Simple wrapper to call sky_template
'''

import os
import sys
import uuid
import logging
import shlex
import subprocess as sproc
import argparse
import numpy as np
import pandas as pd
import multiprocessing as mp
import easyaccess as ea

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

def db_ea(q, dbsection='desoper', drop_dupl=None):
    ''' Method co call easyaccess, lowercase column names and 
    remove duplicates based on a specific column
    '''
    connect = ea.connect(dbsection)
    cursor = connect.cursor()
    outtab = connect.query_to_pandas(q)
    # Transform column names to lower case
    outtab.columns = map(str.lower, outtab.columns)
    # Double check, remove duplicates
    if (drop_dupl is not None):
        outtab.drop_duplicates(drop_dupl, inplace=True)
        outtab.reset_index(drop=True, inplace=True)
    return outtab

def query_pixcor(reqnum, attnum, ccdnum, band):
    ''' Predefined query to get pixcor by band, reqnum, attnum and ccdnum
    '''
    q = 'select e.expnum, fai.path, fai.filename, fai.compression'
    q += ' from file_archive_info fai, pfw_attempt att, desfile d, exposure e' 
    q += ' where att.reqnum={0}'.format(reqnum)
    q += ' and att.attnum={0}'.format(attnum)
    q += ' and fai.desfile_id=d.id'
    q += ' and d.pfw_attempt_id=att.id' 
    q += ' and d.filetype=\'red_pixcor\'' 
    q += ' and CONCAT(\'D00\', e.expnum)=att.unitname' 
    q += ' and fai.filename like \'%_c{0:02}_%\''.format(ccdnum) 
    q += ' and e.band=\'{0}\''.format(band) 
    q += ' order by att.unitname'
    return q

def ccd_call(ccd_info):
    ''' Using info from DB calls sky_template creation per CCD
    '''
    # Uncompress
    reqnum, attnum, ccdnum, band, rootx, label, band_pca, rms = ccd_info
    # Query text
    query = query_pixcor(reqnum, attnum, ccdnum, band)
    # DB
    tab = db_ea(query, dbsection='desoper', drop_dupl=['filename'])
    if (len(tab.index) == 0):
        i = [reqnum, attnum, ccdnum, band]
        logging.error('DB query returned no elements ({0})'.format(i))
    # Construct the full path
    def f1(a, b, c):
        if ((c == None) or (c == -9999)): 
            return os.path.join(rootx, a, b)
        else: 
            return os.path.join(rootx, a, b + c)
    tab['path'] = map(f1, tab['path'], tab['filename'], tab['compression'])
    # Write out a temporary list to call sky_template
    # 1) input list
    fnm_ilist = 'inlist_{0}_'.format(band) + str(uuid.uuid4()) + '.txt'
    tab[['expnum', 'path']].to_csv(fnm_ilist, sep=' ', 
                                   index=False, header=False)
    # 2) config and log
    fnm_config = 'config/' + label + '_c{0:02}_{1}.config'.format(ccdnum, band)
    fnm_log = 'log/' + label + '_c{0:02}_{1}.log'.format(ccdnum, band)
    # 3) out sky template
    fnm_skytemplate = label + '_n04_c{0:02}_{1}.fits'.format(ccdnum, band)
    # 4) good pixels
    fnm_good = 'good_' + fnm_skytemplate
    # 5) PCA for that band
    pca_x = []
    for y in band_pca:
        if ('_{0}_'.format(band) in y):
            pca_x.append(y)
    if (len(pca_x) != 1):
        logging.error('Many/none SKY PCA files: {0}'.format(pca_x))
    pca_x = pca_x[0]
    # Command
    cmd = 'sky_template'
    cmd += ' --saveconfig {0}'.format(fnm_config)
    cmd += ' --log {0}'.format(fnm_log)
    cmd += ' --infile {0}'.format(pca_x)
    cmd += ' --outfilename {0}'.format(fnm_skytemplate)
    cmd += ' --ccdnum {0}'.format(ccdnum)
    cmd += ' --input_list {0}'.format(fnm_ilist)
    cmd += ' --reject_rms {0}'.format(rms[band])
    cmd += ' --good_filename {0}'.format(fnm_good)
    logging.info(cmd)
    cmd = shlex.split(cmd) 
    t1 = 'Calling sky_template generation; {0}-band,'.format(band)
    t1 += ' ccd={0:02}'.format(ccdnum)
    logging.info(t1)
    # Call sky_template
    # pA = sproc.call(cmd, stdout=sproc.PIPE)
    fout = 'stdout_{0}_c{1:02}_{2}.txt'.format(label, ccdnum, band)
    aux_file = open(fout, 'w+')
    pA = sproc.Popen(cmd, stdout=aux_file)#, stdout=sproc.PIPE, shell=True)
    pA.wait()
    # Remove temporary list
    os.remove(fnm_ilist)
    aux_file.close()
    logging.info('Ended {0}, {1}, {2}'.format(label, band, ccdnum))
    return True

def aux_main(reqnum=None, 
             attnum=None, 
             band_list=None, 
             ccd_list=None, 
             skypca_per_band=None, 
             label=None, 
             Nproc=None,
             g_rms=None, 
             r_rms=None, 
             i_rms=None, 
             z_rms=None, 
             Y_rms=None,
             rootx='/archive_data/desarchive/'):
    ''' Main caller 
    '''
    # Parallelislm
    if (Nproc is None):
        Nproc = mp.cpu_count()
    # Label for outputs
    if (label is None):
        label = str(uuid.uuid4())
    # RMS reject value. Updates
    rms = {'g' : 0.008, 'r' : 0.005, 'i' : 0.005, 'z' : 0.003, 'Y' : 0.004}
    if (g_rms is not None):
        rms.update({'g' : g_rms})
    elif (r_rms is not None):
        rms.update({'r' : r_rms})
    elif (i_rms is not None):
        rms.update({'i' : i_rms})
    elif (z_rms is not None):
        rms.update({'z' : z_rms})
    elif (Y_rms is not None):
        rms.update({'Y' : Y_rms})
    # Sky template call is by CCD
    # Parallel call
    P1 = mp.Pool(processes=Nproc)
    runx = []
    for b in band_list:
        logging.info('Band: {0}'.format(b))
        for c in ccd_list:
            runx.append((reqnum, attnum, c, b, rootx, label, skypca_per_band, 
                         rms))
    tmp = P1.map(ccd_call, runx)
    # Pool.map blocks until finished
    #res_band = P1.map(query_pixcor, runx)
    # For the 
    # reqnum, attnum, ccdnum, band, rootx, label, fnm_pca, rms
    logging.info('Ended!')

if __name__ == '__main__':
    blist = ['g', 'r', 'i', 'z']
    clist = np.arange(1, 62 + 1)
    clist = clist[np.where(clist != 61)]
    
    kw = {
        'reqnum' : 3439,
        'attnum' : 1,
        'band_list' : blist,
        'ccd_list' : clist,
        'skypca_per_band' : ['pcamini_Y2Nstack_n04_g_y5.fits', 
                             'pcamini_Y2Nstack_n04_r_y5.fits',
                             'pcamini_Y2Nstack_n04_i_y5.fits',
                             'pcamini_Y2Nstack_n04_z_y5.fits',
                             'pcamini_Y2Nstack_n04_Y_y5.fits'
                            ],
        'label' : 'skytemplate_Y5_Y2Nstack',
    }
    aux_main(**kw)
