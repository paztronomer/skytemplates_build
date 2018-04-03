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
# Global variable for files to delete
TMP_REMOVE = []

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
    TMP_REMOVE.append(fnm_ilist)
    # 2) config and log
    fnm_config = 'config/' + label + '_c{0:02}_{1}.config'.format(ccdnum, band)
    fnm_log = 'log/' + label + '_c{0:02}_{1}.log'.format(ccdnum, band)
    # 3) out sky template
    fnm_skytemplate = label + '_n04_c{0:02}_{1}.fits'.format(ccdnum, band)
    # 4) good pixels
    fnm_good = 'good_' + fnm_skytemplate
    # 5) PCA for that band
    pca_x = band_pca[band]
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
    # fout = 'stdout_{0}_c{1:02}_{2}.txt'.format(label, ccdnum, band)
    # aux_file = open(fout, 'w+')
    # pA = sproc.Popen(cmd)#, stdout=aux_file)#, stdout=sproc.PIPE, shell=True)
    # pA.wait()
    pA = sproc.call(cmd)
    # aux_file.close()
    logging.info('Ended {0}, {1}-band, ccd={2}'.format(label, band, ccdnum))
    return True

def aux_main(reqnum=None, 
             attnum=None, 
             band_list=None, 
             ccd_list=None, 
             skypca_per_band=None, 
             label=None, 
             Nproc=None,
             chunksize=None,
             rms=None, 
             rootx='/archive_data/desarchive/',
             test=False,):
    ''' Main caller 
    '''
    # Parallelislm
    if (Nproc is None):
        Nproc = mp.cpu_count()
    logging.info('Launching {0} processes in parallel'.format(Nproc))
    # Label for outputs
    if (label is None):
        label = str(uuid.uuid4())
    # Sky template call is by CCD
    # Parallel call
    P1 = mp.Pool(processes=Nproc)
    runx = []
    for b in band_list:
        logging.info('Band: {0}'.format(b))
        for c in ccd_list:
            runx.append((reqnum, attnum, c, b, rootx, label, skypca_per_band, 
                         rms))
    if test:
        runx = runx[-20:]
    if (chunksize is not None):
        tmp = P1.map(ccd_call, runx, chunksize)
    else:
        tmp = P1.map(ccd_call, runx)
    try:
        P1.close()
        P1.join()
    except:
        logging.error('Cannot close and join the Pool')
    
    # Pool.map blocks until finished
    #res_band = P1.map(query_pixcor, runx)
    # For the 
    # reqnum, attnum, ccdnum, band, rootx, label, fnm_pca, rms
    #
    # Remove temporary files
    logging.info('Deleting temporary files')
    for t in TMP_REMOVE:
        os.remove(t)
    logging.info('Ended!')

    
if __name__ == '__main__':
    
    hgral = 'Code to call sky_template for a given set of bands and CCDs,'
    hgral += ' using a UNIQUE set of ingredients (same REQNUM, ATTNUM for'
    hgral += ' all the bands). Processes are run in parallel'
    egral = 'Remember to check the produced skytemplates'
    abc = argparse.ArgumentParser(description=hgral, epilog=egral)
    # Band and CCDs
    blist = ['g', 'r', 'i', 'z', 'Y']
    h1 = 'Bands for which to generate the skytemplates. Space separated list.'
    h1 += ' Default: {0}'.format(' '.join(blist))
    abc.add_argument('--band', help=h1, metavar='', default=blist, nargs='+',
                     type=str)
    clist = np.arange(1, 62 + 1)
    clist = clist[np.where(clist != 61)]
    h2 = 'List of CCDs for which to produce skytemplates. Space-separated'
    h2 += ' list. Default: {0}'.format(' '.join( map(str, clist) ))
    abc.add_argument('--ccd', help=h2, metavar='', default=clist, nargs='+',
                     type=int)
    # Run info
    req = 3439
    h3 = 'Reqnum associated to the products (red_pixcor) to be used. Default'
    h3 += ' (Y5): {0}'.format(req)
    abc.add_argument('--req', '-r', help=h3, metavar='', type=int, 
                     default=req)
    att = 1
    h4 = 'Attnum associated to the reqnum for the products (red_pixcor) to'
    h4 += ' be used. Default (Y5): {0}'.format(att)
    abc.add_argument('--att', '-a', help=h4, metavar='', type=int, default=att)
    # Sky PCA per band
    pca = ['pcamini_Y2Nstack_n04_g_y5.fits', 
           'pcamini_Y2Nstack_n04_r_y5.fits',
           'pcamini_Y2Nstack_n04_i_y5.fits',
           'pcamini_Y2Nstack_n04_z_y5.fits',
           'pcamini_Y2Nstack_n04_Y_y5.fits'
          ]
    h5 = 'List of (local) sky PCA files (outputs from sky_pca), for the input'
    h5 += ' bands. Enter on SAME order as the bands. Space-separated list.'
    h5 += ' Default: {0}'.format(' '.join(pca))
    abc.add_argument('--pca', '-p', help=h5, metavar='', default=pca, 
                     nargs='+', type=str)
    raux = [0.008, 0.005, 0.005, 0.003, 0.004]
    h6 = 'Value of RMS reject for each band, in the SAME order as bands are'
    h6 += ' input. Space-separated list.'
    h6 += ' Default: {0}'.format(' '.join( map(str, raux) ))
    abc.add_argument('--val_rms', '-v', help=h6, metavar='', type=float,
                     default=raux, nargs='+')
    # Parallel setup
    h7 = 'Number of processes to be launched in parallel. Default: number of'
    h7 += ' machine cores'
    abc.add_argument('--nproc', help=h7, metavar='', type=int)
    chs = 4
    h8 = 'Chunk size to be used by Pool.map to split the iterable into N'
    h8 += ' chunks which goes to the process pool as separate tasks.'
    h8 += ' Larger chunk sizes would make the code less likeable to incur'
    h8 += ' into memory errors.'
    h8 += ' Default: {0}'.format(chs)
    abc.add_argument('--chunk', help=h8, metavar='', type=int)
    # Label
    pid = 'PID' + str(os.getpid())
    h9 = 'Label to be used by the output files. Default: {0}'.format(pid)
    abc.add_argument('--label', '-l', help=h9, metavar='', type=str,
                     default=pid)
    # Test
    h10 = 'Flag. Whether to launch a test with only 20 images'
    abc.add_argument('--test', help=h10, action='store_true')
    # Parser
    abc = abc.parse_args()
    # Checks
    if (len(abc.band) != len(abc.pca)):
        logging.error('Number of bands needs to be same as PCA files')
        exit(1)
    if (len(abc.band) != len(abc.val_rms)):
        logging.error('Number of bands needs to be same as RMS values')
        exit(1)
    # Construct the 2 dictionaries to be used in base to bands
    pca_kw = dict(zip(abc.band, abc.pca))
    rms_kw = dict(zip(abc.band, abc.val_rms))
    #
    kw = {
        'reqnum' : abc.req,
        'attnum' : abc.att,
        'band_list' : abc.band,
        'ccd_list' : abc.ccd,
        'skypca_per_band' : pca_kw,
        'rms' : rms_kw, 
        'label' : abc.label, #'skytemplate_Y5_Y2Nstack',
        'chunksize' : abc.chunk,
        'Nproc' : abc.nproc,
        'test' : abc.test,
    }
    aux_main(**kw)

