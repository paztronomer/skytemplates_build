""" Quick plot to show the number of selected exposures, per band, as a 
function of T_EFF
"""

import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
# For datetime ticks
import datetime
from matplotlib.dates import MonthLocator, DayLocator, DateFormatter
# For minor ticks 
from matplotlib.ticker import MultipleLocator, FormatStrFormatter
# DES colors
from descolors import BAND_COLORS
# DB
import easyaccess as ea

def dbquery(toquery, dbsection='desoper'):
    connect = ea.connect(dbsection)
    cursor = connect.cursor()
    restab = connect.query_to_pandas(toquery)
    # Map columns to lowercase
    restab.columns = restab.columns.map(str.lower)
    return restab

def aux_query():
    ''' Iteratively get numbers for the number of exposures in each range 
    '''
    #
    def qtext(n1, n2, teff_min, teff_max=2):
        q = "with q as (select expnum, max(lastchanged_time) maxtime"
        q += " from firstcut_eval"
        q += " where analyst!='SNQUALITY'"
        q += " group by expnum"
        q += " )"
        q += " select e.band, e.nite, count(e.expnum) nexposure"
        q += " from q, exposure e, firstcut_eval fcut"
        q += " where nite between {0} and {1}".format(n1, n2)
        q += " and e.exptime between 20 and 90"
        q += " and e.object like '%hex%'"
        q += " and e.expnum=fcut.expnum"
        q += " and fcut.accepted='True'"
        q += " and fcut.program='survey'"
        q += " and fcut.analyst!='SNQUALITY'"
        q += " and fcut.lastchanged_time=q.maxtime"
        q += " and fcut.t_eff>{0}".format(teff_min)
        q += " and fcut.t_eff<{0}".format(teff_max)
        q += " group by e.band, e.nite order by e.band, e.nite"
        return q
    #
    n1, n2 = 20180912, 20181112
    for idx, tx in enumerate(np.arange(0.1, 1., 0.1)):
        if (idx == 0):
            tmp = dbquery(qtext(n1, n2, tx))
            tmp['teff_min'] = tx
        else:
            aux = dbquery(qtext(n1, n2, tx))
            aux['teff_min'] = tx
            tmp = pd.concat([tmp, aux])
    #
    tmp.reset_index(drop=True, inplace=True)
    return tmp

def plot01(df):
    ''' Plot grouped by nite
    '''
    # By grouping, we created an multiindex dataframe, which is handy if used
    # through its hierarchical structure
    dfx = df.groupby(['teff_min', 'band']).agg('sum')
    # To get the values from the different levels of indexing, use 
    # index.get_level_values() 
    #
    # Now, we're only interested in the 'nexposure' column. If we make this 
    # subselection, it will still have the 2 levels of indexig
    dfx = dfx['nexposure']
    index01 = dfx.index.get_level_values('teff_min')
    index02 = dfx.index.get_level_values('band')
    #
    # Plotting
    minorLocator_y = MultipleLocator(50)
    fig, ax = plt.subplots(figsize=(6, 5))
    for b in index02.unique():
        sub = dfx.iloc[index02 == b]
        ax.plot(
            sub.index.get_level_values('teff_min'),
            sub,
            marker='.',
            markersize=8,
            linestyle='-',
            color=BAND_COLORS[b],
            label=b,
        )
    plt.legend(loc='best', fontsize='small', fancybox=True, shadow=True, 
               edgecolor='silver')
    ax.set_xlabel('T_EFF cut value')
    ax.set_ylabel('Total N exposures')
    # Set minot locator for y
    ax.yaxis.set_minor_locator(minorLocator_y)
    # Set grid
    ax.grid(which='minor', alpha=0.3, linestyle='dotted')
    ax.grid(which='major', alpha=0.3, linestyle='dashed')
    # Set horizontal line
    ax.axhline(600, linewidth=1, linestyle='--', color='blue')
    # Title
    ax.set_title('Selected exposures vs varying T_EFF', color='dodgerblue')
    # Spacing adjustment
    plt.subplots_adjust(top=0.95)
    if True:
        outnm = 'Nselect_teff.pdf'
        plt.savefig(outnm, dpi=300, format='pdf')
    plt.show()

def plot02(df):
    ''' For a subset of cut values, plot the total selected exposures per 
    night
    '''
    # Grouping 
    dfy = df.groupby(['teff_min', 'band', 'nite']).agg('sum')
    # Just for clarity, explicit the indices
    index01 = dfy.index.get_level_values('teff_min')
    index02 = dfy.index.get_level_values('band')
    index03 = dfy.index.get_level_values('nite')
    # Range of teff values to plot
    sel_teff = [0.4, 0.5, 0.6]
    # Plotting
    fig, ax = plt.subplots(3, figsize=(7, 8), sharex=True)
    # Setup time locators
    months = MonthLocator()
    days = DayLocator(interval=1)
    days10 = DayLocator(interval=10)
    dateFmt = DateFormatter('%Y%m%d')#('%b%d%y')
    for i, axis in enumerate(ax.flatten()):
        # Setup date axis
        axis.xaxis.set_major_formatter(dateFmt)
        axis.xaxis.set_major_locator(days10)
        axis.xaxis.set_minor_locator(days)
        axis.autoscale_view()
        axis.xaxis.set_tick_params(which='major', 
                                   rotation=30,
                                   width=1.5, 
                                   length=10,
                                  )
        #
        # First selection, by cut value in T_EFF
        tmp1 = dfy.iloc[index01 == sel_teff[i]]
        #Second selection is by band
        tmp1_index02 = tmp1.index.get_level_values('band')
        size = 1
        for b in ['g', 'r', 'i', 'z', 'Y']:# tmp1_index02.unique()[::-1]:
            tmp2 = tmp1.iloc[tmp1_index02 == b]
            # Transform time to datetime
            aux_time = to_datetime(tmp2.index.get_level_values('nite').values)
            # 
            axis.plot(
                np.array(aux_time),
                tmp2.values.flatten(),
                marker='s',
                color=BAND_COLORS[b],
                linestyle='none',
                markeredgecolor='white',
                markeredgewidth=0.5,
                markersize=20 / (size + 1),
                label=b,
            )
            size += 1
        # Label
        axis.legend(loc='best', fontsize='small')
        # Y-axis labels
        axis.set_ylabel('N exposures')
        # Text
        axis.text(0.25, 0.9, 
                  'T_EFF > {0}'.format(sel_teff[i]),
                  color='navy',
                  transform=axis.transAxes)
    # X-axis
    ax[-1].set_xlabel('Night')
    # Title
    plt.suptitle('Number of exposures per band, per night, obeying T_EFF cut',
                 color='dodgerblue')
    # Spacing
    plt.subplots_adjust(left=0.1, bottom=0.13, 
                        right=0.97, top=0.95, 
                        hspace=0.05)
    if True:
        outnm = 'Nselect_perNight.pdf'
        plt.savefig(outnm, dpi=300, format='pdf')
    plt.show()

def to_datetime(arr_nite):
    dateFmt = DateFormatter('%b/%d/%y')
    xdate = [datetime.datetime.strptime(str(date), '%Y%m%d')
             for date in np.sort(arr_nite)]
    return xdate

def aux_main(tab_in=None, ask_db=False):
    ''' Get all together and plot
    '''
    if ask_db:
        df = aux_query()
        df.to_csv('Nselected_teff_PID{0}.csv'.format(os.getpid()), 
                  index=False, header=True)
    else:
        df = pd.read_csv(tab_in)
    # Some info
    print('Nights: {0}'.format(df['nite'].unique()))
    print('T_EFF cuts: {0}'.format(df['teff_min'].unique()))
    print('Bands: {0}'.format(df['band'].unique()))
    # Call plot grouped by nite
    plot02(df)
    plot01(df)


if __name__ == '__main__':
    aux_main(tab_in='Nselected_teff_PID17748.csv')
    
