''' Simple code to add keywords to header
'''

import os
import shutil
import uuid
import numpy as np
import pandas as pd
import multiprocessing as mp
import logging
import argparse
import fitsio
# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
) 

def assemble_fp(in_args,):
    ''' Construct focal plane for a set of input CCDs, using its CCDNUM as
    identifier, and gettig positioning information from a table containing
    CCDNUM, DETSIZE, DETSEC, DATASEC
    After constructig the focal plane, crops the outer border, defined by 
    NaN values
    Inputs
    - fnm_df: dataframe containing a column named 'PATH' with the full paths 
    to the CCD files of the skytemplates (or another set of images to be
    used)
    - i_df: dataframe containing one row per CCD, with minimal set of 
    columns named CCDNUM, DETSIZE, DETSEC, DATASEC
    - pca_comp: number of the PCA component, starting from 0
    - bin_factor: integer being the side of the binned pixels to be created.
    Both axis of the initial CCD needs to be a factor of this value
    - nshort_d0: number of times the shorter axis of the CCD is contained on
    the first dimension of the full focal plane
    - nshort_d1: number of times the shorter axis of the CCD is contained on
    the second dimension of the full focal plane
    - lab: label to be used for output fp filename
    '''
    fnm_df, i_df, pca_comp, bin_factor, lab, nshort_d0, nshort_d1 = in_args
    # Adding a border for safety
    nshort_d0 += 1
    nshort_d1 += 1
    for idx, item in fnm_df.iterrows():
        fnm = item['PATH']
        # Get data and match it with the info table
        hdu = fitsio.FITS(fnm, 'r')
        x_all = hdu[0].read()
        # Work by PCA component. 
        # Shape of the skytemplate array: (4, 4096, 2048)
        if (len(x_all.shape) != 3):
            t_w = 'Must modify the code to use {0}'.format(len(x_all.shape))
            t_w += ' dimensions instead of 3'
            logging.warning(t_w)
        x = x_all[pca_comp, :, :]
        # Check compatibility
        if ((x.shape[0] % bin_factor > 0) or (x.shape[1] % bin_factor > 0)):
            t_w = 'Dimensions of the CCD array are not integer factor of'
            t_w += ' the binning'
            logging.warning(t_w)
        # Bin the CCD
        if (bin_factor is None):
            xbin = x
        else:
            xbin = rebin_mean(x, [i // bin_factor for i in x.shape])
        # Auxiliary funcion. Get list of coordinates from the string list
        def get_list(str_key):
            str_key = str_key.replace('[', '').replace(']', '')
            str_key = str_key.replace(':', ',')
            str_key = map(int, str_key.split(','))
            return str_key
        # Get header information
        h = hdu[0].read_header()
        ccdnum = int(h['CCDNUM'])
        kw1 = i_df.loc[i_df['CCDNUM'] == ccdnum, 'DATASEC']
        if (len(kw1.index) != 1):
            e = 'Info table has not single entry for ccd {0}'.format(ccdnum)
            logging.error(e)
            exit(1)
        # Get the keywords from the table
        detsize = i_df['DETSIZE'].unique()
        detsec = i_df.loc[i_df['CCDNUM'] == ccdnum, 'DETSEC'].values
        datasec = i_df.loc[i_df['CCDNUM'] == ccdnum, 'DATASEC'].values
        if (len(detsize) != 1):
            logging.error('Info table has not single entry for DETSIZE')
            exit(1)
        detsize = get_list(detsize[0])
        detsec = get_list(detsec[0])
        datasec = get_list(datasec[0])
        # Construct the big focal plane as empty array and fill with NaN
        if (bin_factor is None) and (idx == 0):
            s0, s1 = nshort_d0 * min(xbin.shape), nshort_d1 * min(xbin.shape)
            # fp = np.empty((detsize[3], detsize[1]))
            fp = np.empty((s0, s1))
            fp.fill(np.nan)
        elif (bin_factor is not None) and (idx == 0):
            s0, s1 = nshort_d0 * min(xbin.shape), nshort_d1 * min(xbin.shape)
            # fp = np.zeros((detsize[3] // bin_factor + bin_factor, 
            #                detsize[1] // bin_factor + bin_factor))    
            fp = np.empty((s0, s1))
            fp.fill(np.nan)
        # Probable issue. Additional separation between CCDs: datsec vs detsec
        # Insert binned (or not) CCD into the focal plane array
        d0 = np.sort([detsec[2], detsec[3]]) // bin_factor
        d1 = np.sort([detsec[0], detsec[1]]) // bin_factor
        aux0 = min(d0)
        aux1 = min(d1)
        fp[aux0:aux0 + xbin.shape[0], aux1:aux1 + xbin.shape[1]] = xbin
    # Remove border of non-CCD pixels in the fp.
    fp = remove_border(fp, rm_value=np.nan)
    # Save output
    if (lab is None):
        lab = str(uuid.uuid4())
    outfnm_fp = '{0}_PCA{1}_fp.fits'.format(lab, pca_comp)
    if os.path.exists(outfnm_fp):
        t_err = 'File {0} already exists. Not overwritting'.format(outfnm_fp)
        logging.warning(t_err)
    else:
        fits = fitsio.FITS(outfnm_fp, 'rw')
        fits.write(fp) 
        fits.close()
        logging.info('{0} saved'.format(outfnm_fp))
    return True

def remove_border(arr, rm_value=np.nan):
    ''' Function to remove the outer box, containing only values 
    defined by rm_value. Work only in 2 dimensions.
    Inputs 
    - arr: array to be cropped
    - rm_value: value to define the border
    Returns
    - cropped array
    '''
    if (arr.size < 9):
        logging.warning('Array has less than 9 pixels')
    # Get booleans 1D for the columns/rows that have exclusively NaN
    if np.isnan(rm_value):
        bord_d1 = np.all(np.isnan(arr), axis = 0)
        bord_d0 = np.all(np.isnan(arr), axis = 1)
    else:
        t_e = 'Not yet implemented. Exiting'
        logging.error(t_e)
        exit()
    # Identify the borders, asking to not have other values outside them
    logging.info('{0} {1}'.format(bord_d0.shape, bord_d1.shape))
    idx_min_d0 = 0
    idx_max_d0 = arr.shape[0]
    i = 1
    while True:
        if ((bord_d0[0]) and (bord_d0[i]) and (bord_d0[i - 1])):
            idx_min_d0 = i
        else:
            break
        i += 1
    j = bord_d0.size - 2
    while True:
        if ((bord_d0[-1]) and (bord_d0[j]) and (bord_d0[j + 1])):
            idx_max_d0 = j
        else: 
            break
        j -= 1
    idx_min_d1 = 0
    idx_max_d1 = arr.shape[1]
    k = 1
    while True:
        if ((bord_d1[0]) and (bord_d1[k]) and (bord_d1[k + 1])):
            idx_min_d1 = k
        else:
            break
        k += 1
    m = bord_d1.size - 2
    while True:
        if ((bord_d1[-1]) and (bord_d1[m]) and (bord_d1[m + 1])):
            idx_max_d1 = m
        else: 
            break
        m -= 1
    # Using the above indices, crop the initial array
    res = arr[idx_min_d0:idx_max_d0 + 1, idx_min_d1:idx_max_d1 + 1]
    return res

def rebin_mean(arr, new_shape):
    '''Rebin 2D array arr to shape new_shape by averaging
    https://scipython.com/blog/binning-a-2d-array-in-numpy/
    '''
    shape = (new_shape[0], arr.shape[0] // new_shape[0],
             new_shape[1], arr.shape[1] // new_shape[1])
    return arr.reshape(shape).mean(-1).mean(1)

def modif_header(fnm, i_tab, out_dir=None): 
    ''' Function to modify the header, based on an input table containing
    the neccesary information
    Inputs
    - fnm: fits filename
    - i_tab: filename of the table containign CCD identifier plus the 
    positioning keywords
    - out_dir: directory where to put the modified FITS
    '''
    # NOTE: This function was developed to use swarp after the header was 
    # changed. But swarp did not used the pixel coordinates as expected
    #
    # If the path has only the filename, then the first element of 
    # os.path.split is a empty string
    if (out_dir is None):
        outf = os.path.join(os.path.split(fnm)[0], 
                            'copy_' + os.path.split(fnm)[1])
    else:
        outf = os.path.join(out_dir, 'copy_' + os.path.split(fnm)[1])
    # Copy the initial file and its metadata, to modify it
    shutil.copy(fnm, outf)
    shutil.copystat(fnm, outf)
    # Get data and match it with the info table
    hdu = fitsio.FITS(outf, 'rw')
    x = hdu[0].read()
    h = hdu[0].read_header()
    ccdnum = int(h['CCDNUM'])
    kw1 = i_tab.loc[i_tab['CCDNUM'] == ccdnum, 'DATASEC']
    if (len(kw1.index) != 1):
        e = 'Info table has not single entry for ccd {0}'.format(ccdnum)
        logging.error(e)
        exit(1)
    hlist = [{'name' : 'DATASEC', 'value' : kw1.values[0], 
              'comment' : 'Added by Francisco PCh'}]
    hdu[0].write_keys(hlist)
    hdu.close()
    logging.info('{0} successfully changed'.format(outf))

def aux_main(path_fnm, info_fnm,
             N_PCA=4, 
             bin_factor=None,
             nshort_d0=14, 
             nshort_d1=12, 
             lab=None,):
    ''' Auxiliary main
    '''
    # Load table of paths and the information table
    df_info = pd.read_csv(info_fnm)
    df_path = pd.read_csv(path_fnm, names=['PATH'])
    # Setup parallel call
    t_i ='Setup parallel call in {0} processes.'.format(N_PCA)
    t_i += ' This machine has {0} processors'.format(mp.cpu_count())
    logging.info(t_i)
    P1 = mp.Pool(processes=N_PCA)
    aux_call = []
    for n in range(N_PCA):
        aux_call.append([df_path, df_info, n, 
                         bin_factor, 
                         lab, nshort_d0, nshort_d1])
    P1.map(assemble_fp, aux_call)
    # The map() method locks the pool until completed
    logging.info('Completed!')


if __name__ == '__main__':
    t0 = 'Add keywords based on external table, to a set of skytemplates'
    uno = argparse.ArgumentParser(description=t0)
    t1 = 'List containing one full path per line, for each of the CCDs'
    uno.add_argument('path_list', help=t1)
    t2 = 'Table containing the keywords per CCD, along with a CCD'
    t2 += ' identificator'
    uno.add_argument('info_table', help=t2)
    # 
    npca = 4
    t3 = 'Number of components the skytemplates PCA decomposition has.'
    t3 += ' Default: {0}'.format(npca)
    uno.add_argument('--npca', help=t3, type=int, default=npca)
    binf = 4
    t4 = 'Bin factor to be used for both axis of the input CCDs. Remember '
    t4 += ' CCD shape needs to be factorable by this value. Default:'
    t4 += ' {0}'.format(binf)
    uno.add_argument('--bin', help=t4, type=int, default=binf)
    t5 = 'Label used fro output FITS focal plane images. Default is a UUID'
    uno.add_argument('--lab', help=t5)
    short0 = 14
    t6 = 'Number of times the short axis length fits into FIRST dimension of'
    t6 += ' the focal plane, when CCDs have its longest axis in vertical.'
    t6 += ' Default: {0}'.format(short0)
    uno.add_argument('--n0', help=t6, type=int, default=short0)
    short1 = 12
    t7 = 'Number of times the short axis length fits into SECOND dimension of' 
    t6 += ' the focal plane, when CCDs have its longest axis in vertical.'
    t6 += ' Default: {0}'.format(short1)
    uno.add_argument('--n1', help=t7, type=int, default=short1)
    # 
    uno = uno.parse_args()
    #
    aux_main(uno.path_list, uno.info_table, N_PCA=uno.npca, 
             bin_factor=uno.bin, nshort_d0=uno.n0, nshort_d1=uno.n1,
             lab=uno.lab)
