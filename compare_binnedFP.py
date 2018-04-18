''' Simple script to compare binned_fp images from DECam, generates the 
residual from the difference and some statistics
'''

import os
import argparse
import numpy as np
import pandas as pd
import scipy.signal 
import logging
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable
import fitsio
# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)


def open_binned(fname):
    hdu = fitsio.FITS(fname)
    x = np.copy(hdu[0].read())
    hdu.close()
    return x

def stat_binned(fnm1, fnm2, m):
    ''' Do the statistics for the 2 binned focal plane FITS files, save the 
    stats and the residual of the comparison/difference
    '''
    x1, x2 = open_binned(fnm1), open_binned(fnm2)
    # Some data
    expnum = int(fnm1[1 : 9])
    band = fnm1[10]
    # Mask both arrays 
    x1_msk = np.ma.masked_where(np.logical_or(x1 == -1, x2 == -1), x1)
    x2_msk = np.ma.masked_where(np.logical_or(x1 == -1, x2 == -1), x2)
    if (m == 'diff'):
        r = x1_msk - x2_msk
    elif (m == 'div'):
        r = x1_msk / x2_msk
    
    # Correlation gave me the same answer for all the cases, maybe because 
    # of the range of variation
    # Correlation between both of them. Remember the outer region will 
    # exactly match
    # cc = scipy.signal.correlate2d(x1_msk, x2_msk, mode='full', 
    #                               boundary='fill', fillvalue=-1)
    # cc = cc / np.median(cc)

    plt.close('all')
    fig, ax = plt.subplots(2, 2, figsize=(8, 7))
    #
    k_im1 = {
        'origin' : 'lower',
        'vmin' : np.percentile(r, 5),
        'vmax' : np.percentile(r, 95),
        'cmap' : 'bwr',
    }
    im1 = ax[0, 0].imshow(r, **k_im1)
    #
    aux_r = r.compressed()
    w = np.ones_like(aux_r) / aux_r.size
    k_h = {
        'range' : [np.percentile(r, 5), np.percentile(r, 95)],
        'histtype' : 'step',
        'weights' : w,
    }
    ax[0, 1].hist(aux_r, color='olive', lw=2, bins=15, **k_h) 
    ax[0, 1].hist(aux_r, cumulative=True, color='k', lw=1, linestyle='--', 
               bins=800, **k_h)
    #
    z_aux = np.r_[x1_msk.compressed(), x2_msk.compressed()]
    k_im3 = {
        'origin' : 'lower',
        'vmin' : np.percentile(z_aux, 5),
        'vmax' : np.percentile(z_aux, 95),
        'cmap' : 'gray_r',
    }
    im3 = ax[1, 0].imshow(x1_msk, **k_im3)
    im4 = ax[1, 1].imshow(x2_msk, **k_im3)

    # Setup colorbars
    divider0 = make_axes_locatable(ax[0, 0])
    cax0 = divider0.append_axes('right', size='5%', pad=0.05)
    plt.colorbar(im1, cax=cax0)
    divider2 = make_axes_locatable(ax[1, 0])
    cax2 = divider2.append_axes('right', size='5%', pad=0.05)
    plt.colorbar(im3, cax=cax2)
    divider3 = make_axes_locatable(ax[1, 1])
    cax3 = divider3.append_axes('right', size='5%', pad=0.05)
    plt.colorbar(im4, cax=cax3)

    # Title
    txt = 'Expnum:{0}, {1}-band\nComparing skyfits'.format(expnum, band)
    txt += ' constructed with Y5E1 vs Y5all skytemplates (Y5E2 starflats)'
    txt += '\nAvg={0:.2f} Median={1:.2f}'.format(np.mean(aux_r), 
                                                 np.median(aux_r))
    txt += ' RMS={0:.2f}'.format(np.sqrt(np.mean(aux_r * aux_r)))
    plt.suptitle(txt, color='b')
    ax[0, 0].set_title(r'Ratio: $\frac{skyfit\ with\ Y5E1}{skyfit\ with\ Y5all}$')
    ax[0, 1].set_title('Histogram & Cumulative dist.')
    ax[1, 0].set_title('Skyfit with Y5E1 skytemplate')
    ax[1, 1].set_title('Skyfit with Y5all skytemplate')

    # Spacing
    plt.subplots_adjust(left=0.05, bottom=0.05, right=0.95, top=0.85, 
                        wspace=0.35, hspace=0.35)
    # Save/show
    if True:
        outnm = 'expnum{0}_{1}_skyfitCompare.pdf'.format(expnum, band)
        plt.savefig(outnm, dpi=300, format='pdf')
    else:
        plt.show()

def aux_main(list1, list2, m):
    l1 = pd.read_table(list1, names=['expnum'])['expnum'].values
    l2 = pd.read_table(list2, names=['expnum'])['expnum'].values
    if (l1.size == l2.size):
        pass
    else:
        logging.error('Input lists doesn\'t have same amount of elements')
        exit(1)
    res = []
    for idx in range(len(l1)):
        res.append(stat_binned(l1[idx], l2[idx], m))

if __name__ == '__main__':
    gral = 'Simple script to compare binned_fp images from DECam, generates'
    gral += ' the residual from the difference and some statistics' 
    arg = argparse.ArgumentParser(description=gral)
    h0 = 'List of first set of binned_fp to compare'
    arg.add_argument('x1', help=h0)
    h1 = 'List of second set of binned_fp to compare'
    arg.add_argument('x2', help=h1)
    h2 = 'Which method to use for compare set of binned_fp. Options are'
    h2 += ' \'diff\' or \'div\'. Default: \'div\''
    arg.add_argument('-m', help=h2, metavar='', choices=['diff', 'div'],
                     default='div')
    arg = arg.parse_args()
    aux_main(arg.x1, arg.x2, arg.m)


