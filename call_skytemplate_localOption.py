''' Simple wrapper to call sky_template, with modifications added to copy
files to local directory
'''

import os
import sys
import time
import uuid
import logging
import shlex
import shutil
import subprocess as sproc
import argparse
import itertools
import numpy as np
import pandas as pd
import multiprocessing as mp
import easyaccess as ea

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

def copy2_local(kw):
    ''' Method to copy files preserving metadata
    Inputs
    kw: list
        List containing the source file and the destination path
    '''
    ini, end = kw
    shutil.copy2(ini, end)
    return True

def copy_local(source, dest, nproc=None, chunk=None):
    ''' Function to copy in parallel to a local destination
    Inputs
    source: list
        List of the source paths, pointing to the files, not just directories
    dest: string
        Unique destination path to all the input files
    nproc: int
        Number of processes to be launched in parallel. If None, then the number
        of CPUs will be used
    '''
    if (nproc is None):
        nproc = mp.cpu_count()
    aux_path = zip(source, [dest] * len(source))
    Pz = mp.Pool(processes=nproc)
    if (chunk is None):
        res = Pz.map(copy2_local, aux_path)
    else:
        res = Pz.map(copy2_local, aux_path, chunk)
    Pz.close()
    return

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

def ccd_copy(kw_ccd):
    ''' Copy files to a local directory, using a predefined tree-structure
    '''
    # Uncompress
    reqnum, attnum, ccdnum, band, rootx, save_dir, nproc, chsize = kw_ccd
    # Fill the query text
    query = query_pixcor(reqnum, attnum, ccdnum, band)
    # DB query, droping duplicates in filename
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
    # Steps to save files and keep a list of the files to be used
    # 1) Filename of the list with the paths to be used when locally call
    # skytemplate
    fnm_ilist = 'inlist_{0}_r{1}p{2:02}_c{3:02}.txt'.format(band, reqnum,
                                                            attnum, ccdnum)
    fnm_ilist = os.path.join(save_dir, fnm_ilist)
    # 2) Directory to harbor the CCD files
    tmp_dest = '{0}_r{1}p{2:02}_c{3:02}'.format(band, reqnum, attnum, ccdnum)
    tmp_dest = os.path.join(save_dir, tmp_dest)
    # 3) Check directories existence
    if not os.path.exists(save_dir):
        try:
            os.mkdir(save_dir)
        except:
            logging.error('Exception: {0}'.format(sys.exc_info()[0]))
            logging.info('{0} could not be created'.format(save_dir))
            exit(1)
    else:
        logging.info('Path already exists {0}'.format(save_dir))
    if not os.path.exists(tmp_dest):
        try:
            os.mkdir(tmp_dest)
        except:
            logging.error('Exception: {0}'.format(sys.exc_info()[0]))
            logging.info('{0} could not be created'.format(tmp_dest))
            exit(1)
    else:
        logging.info('Path already exists {0}'.format(tmp_dest))
    # 4) Construct the paths were the CCD files will be saved. Write out the
    # list
    files_immask = list(
        map(os.path.join,
            [tmp_dest] * len(tab['path']),
            map(os.path.basename, tab['path'])
        )
    )
    flist = pd.DataFrame({'01_expnum': tab['expnum'], '02_path': files_immask})
    flist.to_csv(fnm_ilist, sep=' ',index=False, header=False)
    # 5) Call the copy of files, in parallel
    logging.info('Start copying CCD={0} files'.format(ccdnum))
    copy_local(list(tab['path'].values), tmp_dest, nproc=nproc, chunk=chsize)
    logging.info('CCD={0} copy finished'.format(ccdnum))
    return True

def ccd_call(kwinfo):
    ''' Calling of sky_template creation, per CCD. It can run either from
    local files or from DB queries
    Inputs
    reqnum: int
        Reqnum as listed in DB
    attnum: int
        Attnum as listed in the DB
    ccdnum: int
        CCD number as listed in the DB
    band: str
        Band as listed in the DB
    rootx: str
        Root path to be prefixed to the paths obtained from DB. Default will be
        '/archive_data/desarchive/'
    label: str
        Lael to use for naming outputs
    band_pca: dict
        As {'g': 'pca_mini_y6_e1_g_n04.fits'}
    rms: dict
        As {'g': 0.008}
    runLocal: bool
        To run or not locally
    dirx: str
        Directory where (in case of running local) the subdirectories for each
        CCD are located
    sh_dir: str
        Parent directory where to store the bash files for calling skytemplates
        by hand
    '''
    # Get the arguments
    reqnum, attnum, ccdnum, band = kwinfo[:4]
    rootx, label, band_pca, rms, runLocal, dirx, sh_dir = kwinfo[4:]

    # 1) List of files from which to run skytemplate generation
    if runLocal:
        fnm_ilist = 'inlist_{0}_r{1}p{2:02}_c{3:02}.txt'.format(band, reqnum,
                                                                attnum, ccdnum)
        fnm_ilist = os.path.join(dirx, fnm_ilist)
    else:
        # Fill up the query text
        query = query_pixcor(reqnum, attnum, ccdnum, band)
        # DB query
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
        fnm_ilist = 'inlist_{0}_'.format(band) + str(uuid.uuid4()) + '.txt'
        tab[['expnum', 'path']].to_csv(fnm_ilist, sep=' ',
                                       index=False, header=False)

    # 2) Define config and log output filenames
    fnm_config = 'config/' + label + '_c{0:02}_{1}.config'.format(ccdnum, band)
    fnm_log = 'log/' + label + '_c{0:02}_{1}.log'.format(ccdnum, band)
    # 3) Define the output filename sky template
    fnm_skytemplate = label + '_n04_c{0:02}_{1}.fits'.format(ccdnum, band)
    # 4) Define filename for good pixels file
    fnm_good = 'good_' + fnm_skytemplate
    # 5) Get the PCA filename for that band
    pca_x = band_pca[band]
    # 6) Construct the command
    cmd = 'sky_template'
    cmd += ' --saveconfig {0}'.format(fnm_config)
    cmd += ' --log {0}'.format(fnm_log)
    cmd += ' --infile {0}'.format(pca_x)
    cmd += ' --outfilename {0}'.format(fnm_skytemplate)
    cmd += ' --ccdnum {0}'.format(ccdnum)
    cmd += ' --input_list {0}'.format(fnm_ilist)
    cmd += ' --reject_rms {0}'.format(rms[band])
    cmd += ' --good_filename {0}'.format(fnm_good)
    # 7) Save the command into a txt file
    sh_name = '{0}_r{1}p{2:02}.sh'.format(band, reqnum, attnum, ccdnum)
    sh_name = os.path.join(sh_dir, sh_name)
    if not os.path.exists(sh_dir):
        try:
            os.mkdir(sh_dir)
        except:
            logging.error('Exception: {0}'.format(sys.exc_info()[0]))
            logging.info('{0} could not be created'.format(sh_dir))
            exit(1)
    else:
        logging.info('Path already exists {0}'.format(sh_dir))
    if os.path.exists(sh_name):
        with open(sh_name, 'a+') as f:
            f.write(cmd + '\n')
    else:
        with open(sh_name, 'w+') as f:
            f.write(cmd + '\n')
    logging.info('Command saved into {0}'.format(sh_name))

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
    pA.close()
    #
    # If called from DB, here must delete the files used for listing the
    # skytemplate ingredients
    #
    if (not runLocal):
        try:
            os.remove(fnm_ilist)
        except:
            logging.warning('Failed to delete {0}'.format(fnm_ilist))

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
             test=False,
             local_copy=False,
             local_run=False,
             dest_dir='immasked/',
             bash_dir='bash_skytmp/',):
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
    #
    # Different calls: copy files / run from desarchive / run from local
    #
    if local_copy:
        # Parallel setup for copy is on transfer level, not at querying
        for b in band_list:
            logging.info('Band: {0}'.format(b))
            for c in ccd_list:
                logging.info('CCD: {0}'.format(c))
                ccd_copy((reqnum, attnum, c, b, rootx, dest_dir,
                          Nproc, chunksize))
    else:
        # Parallel call either for local run or for running from desarchive
        # The option 'local_run' will give the ability of run from local files
        # and lists, specified by band-reqnum-attnum-ccdnum
        P1 = mp.Pool(processes=Nproc)
        runx = []
        for b in band_list:
            logging.info('Band: {0}'.format(b))
            for c in ccd_list:
                runx.append((reqnum, attnum, c, b,
                             rootx, label, skypca_per_band,
                             rms, local_run, dest_dir, bash_dir))
        if test:
            runx = runx[-20:]
        try:
            if (chunksize is not None):
                tmp = P1.map(ccd_call, runx, chunksize)
            else:
                tmp = P1.map(ccd_call, runx)
            try:
                P1.close()
                P1.join()
            except:
                logging.error('Pool cannot be closed')
        except:
            logging.error('Call of Pool.map failed')
        finally:
            logging.info('Remember to free up space deleting tmp files')
            # Remove temporary files
            # logging.info('Deleting temporary files')
            # for t in tmp:
            #     try:
            #         os.remove(t)
            #     except:
            #         logging.warning('Failed to delete {0}'.format(t))
    logging.info('Ended!')


if __name__ == '__main__':

    hgral = 'Code to call sky_template for a given set of bands and CCDs,'
    hgral += ' using a UNIQUE set of ingredients (same REQNUM, ATTNUM for'
    hgral += ' all the bands). Processes are run in parallel. If running from'
    hgral += ' local files, it uses a predefined directory tree schema'
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
    pca = ['pca_Y2Nstack_y6e1_g_n04.fits',
           'pca_Y2Nstack_y6e1_r_n04.fits',
           'pca_Y2Nstack_y6e1_i_n04.fits',
           'pca_Y2Nstack_y6e1_z_n04.fits',
           'pca_Y2Nstack_y6e1_Y_n04.fits'
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
    # Local copy
    h10a = 'Flag to copy pixcorrect CCD files to local direcory \'immasked/\''
    abc.add_argument('--copy', help=h10a, action='store_true')
    # Local run
    h10b = 'Flag to run from local files. It uses a defined directory tree'
    h10b += ' based on reqnum, attnum, band, and ccdnum:'
    h10b += ' \'immasked/{BAND}_r{REQNUM}p{ATTNUM}_c{CCDNUM}/\''
    abc.add_argument('--local', help=h10b, action='store_true')
    #
    # Test
    h11 = 'Flag. Whether to launch a test with only 20 images'
    abc.add_argument('--test', help=h11, action='store_true')
    # Parser
    abc = abc.parse_args()
    # Checks
    if ((len(abc.band) != len(abc.pca)) and (not abc.copy)):
        logging.error('Number of bands needs to be same as PCA files')
        exit(1)
    if ((len(abc.band) != len(abc.val_rms)) and (not abc.copy)):
        logging.error('Number of bands needs to be same as RMS values')
        exit(1)
    # Construct the 2 dictionaries to be used in base to bands
    pca_kw = dict(zip(abc.band, abc.pca))
    rms_kw = dict(zip(abc.band, abc.val_rms))
    #
    kw = {
        'reqnum': abc.req,
        'attnum': abc.att,
        'band_list': abc.band,
        'ccd_list': abc.ccd,
        'skypca_per_band': pca_kw,
        'rms': rms_kw,
        'label': abc.label, #'skytemplate_Y5_Y2Nstack',
        'chunksize': abc.chunk,
        'Nproc': abc.nproc,
        'test': abc.test,
        'local_copy': abc.copy,
        'local_run': abc.local,
    }
    aux_main(**kw)

    ''' Example:
         Namespace(att=1, band=['g'], ccd=array([ 1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17,
           18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
           35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
           52, 53, 54, 55, 56, 57, 58, 59, 60, 62]), chunk=15, label='test_skytmp_y6e1_g', nproc=2, pca=['pca_mini_y6_e1_g_n04.fits'], req=3914, test=False, val_rms=[0.008])
    '''
