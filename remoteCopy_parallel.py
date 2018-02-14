''' Francisco's code to remote copy a set of exposures. For convenience
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
                check_file=True,
                land_folder='/home/fpazch/scratch/skytemplates_files/raw'):
    ''' Function to remote copy raw files from  NCSA to local. Remember to
    deal with iterative password input or ssh-keygen + ssh-copy-id
    '''
    cmd = 'scp {0} {1}'.format(tmp_f, land_folder)
    # Check if file was already remote copied. This is useful for re-runs
    disk_file = os.path.join(land_folder, os.path.basename(tmp_f))
    if os.path.exists(disk_file):
        # If file is there, do not copy
        logging.info('File {0} is already on disk'.format(disk_file))
        return True
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
    try:
        pool1 = Pool(processes=nproc)
        logging.info('Running scp in parallel')
        pool1.map(remote_copy, list_raw)
        # pool1.join()
        pool1.close()
    except:
        e = sys.exc_info()[0]
        logging.error(e)
        
    # Finishing
    t1 = time.time()
    logging.info('Ended in {0:.2f} minutes'.format( (t1 - t0) / 60.))
