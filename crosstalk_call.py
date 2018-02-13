''' Francisco's code to call crosstalk for a set of exposures. For convenience
classes will not be used, to easily launch parallelism
'''

import os
import sys
import time
import socket
import shlex
import subprocess
import logging
import argparse
import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool, Process

def setup_log(log_path):
    ''' Function to setup the log file, from the input path
    '''
    # Check if parent path exists
    if os.path.exists(os.path.dirname(log_path)):
        pass
    else:
        # Try creating the path
        try:
            os.makedirs(os.path.dirname(log_path))
        except:
            e = sys.exc_info()[0]
            print('ERROR: {0}'.format(e))
            exit(1)
    logging.basicConfig(filename=log_path,
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s'
                       )
    return True

def remote_copy(tmp_f, 
                land_folder='/home/fpazch/scratch/skytemplates_files/raw'):
    ''' Function to remote copy raw files from  NCSA to local. Remember to
    deal with iterative password input or ssh-keygen + ssh-copy-id
    '''
    cmd = 'scp {0} {1}'.format(tmp_f, land_folder)
    # Check if destination directory exists
    if os.path.exists(land_folder):
        pass
    else:
        # Try creating the path
        try:
            os.makedirs(land_folder)
        except:
            e = sys.exc_info()[0]
            logging.error(e)
            exit(1)
    proc = subprocess.Popen(shlex.split(cmd),
                            shell=False, 
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True,
                           )
    proc.wait()
    stderr = proc.stderr.read()
    if stderr:
        logging.error('Error when copying: {0}'.format(tmp_f))
        logging.error(stderr)
    else:
        logging.info('Remote copied: {0}'.format(os.path.basename(tmp_f)))
    del proc
    return True

def copy_list(csv_table, 
              remote_user = 'fpazch@deslogin.cosmology.illinois.edu',
              parent='/archive_data/desarchive/DTS/raw'):
    ''' Method to construct the list of files to be remote copied. The input
    csv file must have exposure number and night as aprt of the columns, with
    the names of the columns 
    '''
    df = pd.read_csv(csv_table, sep=None, engine='python')
    df.columns = map(str.upper, df.columns)
    mincol = ['EXPNUM', 'NITE', 'BAND']
    X = set(mincol).issubset( set(list(map(str.upper, df.columns.values))) )
    if X:
        path_list = []
        for ind, row in df.iterrows():
            aux_p = os.path.join(parent, str(row['NITE']))
            aux_p = os.path.join(aux_p, 
                                 'DECam_{0:08}.fits.fz'.format(row['EXPNUM'])
                                )
            aux_p = remote_user + ':' + aux_p
            path_list.append(aux_p)
    else:
        errm = 'Input file does not have the minimal set of columns'
        logging.error(errm)
    logging.info('List of files to be copied was successfully created')
    return (path_list, df)

def xtalk_call((expnum, band), 
               ftype='xtalked_sci',
               raw_path='/home/fpazch/scratch/skytemplates_files/raw',
               out_path='/home/fpazch/scratch/skytemplates_files/xtalked_sci',
               config_path='/home/fpazch/scratch/skytemplates_files/config',
               log_path='/home/fpazch/scratch/skytemplates_files/log',
               xt_matrix='DECam_20130606_TEST3B2B.xtalk',
               hupdate='20151008_DES_header_update.20140303',
               ):
    ''' Function to call DECam_crosstalk, using as a template the CMDARGS in
    PFW_EXEC, used for supercal_20160921t1003, attempt=2, reqnum=3335
    One file at a time
    '''
    p0 = 'DECam_crosstalk'
    #
    aux0 = os.path.join(raw_path, 
                        'DECam_{0:08}.fits.fz'.format(expnum)
                       )
    p0 += ' {0}'.format(aux0)
    #
    aux1 = 'D{0:08}_{1}_c\%02d_xtalked_sci.fits'.format(expnum, 
                                                        band,
                                                        ftype)
    aux1 = os.path.join(out_path, aux1)
    p0 += ' {0}'.format(aux1)
    #
    aux2 = '-ccdlist 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,'
    aux2 += '21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,'
    aux2 += '41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,'
    aux2 += '62'
    p0 += ' {0}'.format(aux2)
    #
    aux3 = os.path.join(config_path, xt_matrix)
    aux3 = '-crosstalk {0}'.format(aux3)
    p0 += ' {0}'.format(aux3)
    #
    aux4 = '-overscanfunction 0  -overscanorder 1  -overscansample 1'
    aux4 +='  -overscantrim 5 -photflag 1'
    p0 += ' {0}'.format(aux4)
    #
    aux5 = os.path.join(config_path, hupdate)
    aux5 = '-replace {0}'.format(aux5)
    p0 += ' {0}'.format(aux5)
    #
    p0 += ' -verbose 3'
    #
    aux6 = 'xtalk_D{0:08}_{1}.log'.format(expnum, band)
    aux6 = os.path.join(log_path, aux6)
    notp0 = ' > {0} 2>&1'.format(aux6)
    #
    print(p0)
    xt_proc = subprocess.Popen(shlex.split(p0),
                               shell=False,#shell=True, 
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               universal_newlines=True,
                              )
    xt_proc.wait()
    print(xt_proc.stdout.read())
    stderr = xt_proc.stderr.read()
    if stderr:
        logging.error('Error in crosstalk: {0}'.format(aux0))
        logging.error(stderr)
    else:
        logging.info('Crosstalked: {0}'.format(aux0))
    del xt_proc
    return True

if __name__ == '__main__':
    t0 = time.time()
    # By convenience
    if (sys.argv[1] in ('-h', '--help')):
        print('USAGE: python code.py table.csv name_log.log')
        exit(0)
    csv = sys.argv[1]
    logname = sys.argv[2]
    nproc = mp.cpu_count() - 1

    # Setup log
    path1 = '/home/fpazch/scratch/skytemplates_files/log'
    path1 = os.path.join(path1, logname)
    setup_log(path1)

    # Get paths for remote copy
    (list_raw, df_info) = copy_list(csv)
    # List of tuples for xtalk
    exp_band = list( zip(df_info['EXPNUM'], df_info['BAND']) )

    # Pool of workers
    pool1 = Pool(processes=nproc)
    logging.info('Running scp in parallel')
    pool1.map(remote_copy, list_raw)
    pool1.join()
    pool1.close()

    pool2 = Pool(porcesses=nproc)
    logging.info('Running crosstalk in parallel')
    pool2.map(xtalk_call, exp_band)
    pool2.join()
    pool2.close()

    # Finishing
    t1 = time.time()
    logging.info('Ended in {0:.2f} minutes'.format( (t1 - t0) / 60.))
