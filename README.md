# skytemplates_build :wrench:

## Routines for skytemplates generation
The **skytemplates** generation consists in 4 main steps: selection of images,
run few processing steps to generate the products to be used as ingredients,
run of **sky_pca** to produce the number of PCA components, and
**sky_template** to generate the final product, per band, per CCD.


### Selection of science images

1. Get range of nights to work on (given by **epoch** range)

1. Select object images with typical exposure time, not being part of the SN
survey, and reasonably good `T_EFF` (measured from `B_EFF` and `C_EFF`). Notice
that we need to decrease/increase the threshold value depending on the band, to
get about 600 exposures
   ```SQL
     with w as (select expnum, max(lastchanged_time) maxtime
                from finalcut_eval
                where analyst!='SNQUALITY' group by expnum)
     select e.expnum, e.nite, e.exptime, e.band, e.object, fcut.t_eff
     from w, exposure e, finalcut_eval fcut
     where nite between NIGHT_START and NIGHT_END
        and exptime between 20 and 100
        and e.object like '%hex%'
        and e.band={band}
        and e.expnum=fcut.expnum
        and fcut.accepted='True'
        and fcut.program='survey'
        and fcut.analyst != 'SNQUALITY'
        and fcut.lastchanged_time=w.maxtime
        and fcut.t_eff > 0.9
        order by fcut.t_eff; > explist_{year}_{epoch}_{band}.csv
   ```
1. With the list of exposures, apply further selection criteria. Example: avoid
the vicinity of 20171110 when the *mysterious* lightbulb appeared.

### Run creation of ingredients, through few blocks of the pipeline

1. Run the following steps
    1. crosstalk-sci
    1. pixcorrect (save outputs)
    1. (not-essential:) wcs + mkbleedmask (in case I want to experiment with
      few images)
    1. skycompress-mkbleedmask
    1. skycombine-mkbleedmask (save outputs)
    * **Note** as we are stacking many exposures, make bleed masks can be
    avoided, just as a statistic consequence.

1. **Version without mkbleedmask :**
I modified a template for *widefield* to only run the above steps. As these
steps assume *mkbleedmask* ran, I modified the *wcl* files to use *pixcorrect*
outputs. The *bleedmask-mini* are not specifically generated from *bleedmasked*
but from *pixcorrected*. The campaign for this are `Y4A1_PRESKY` and
`Y5N_PRESKY`

1. **Version with mkbleedmask :**
For the version using *mkbleedmask*, it also needs information from the WCS to
be calculated beforehand (SExtractor). I don't have the *campaign* to do this

1. Get the list of exposure numbers from the .csv created few steps ago
    ```
    awk -F "," '{print $1}' {some csv file}
    cat {some csv file} | cut -d, -f1
    ```
1. A typical call would be. Note if you use `expnum` instead of `list` it triggers an error
    ```
    submit_widefield.py
    --db_section db-desoper
    --list {expnum list}
    --campaign {Y4A1_PRESKY, Y5N_PRESKY}
    --project {ACT, OPS}
    --target_site CampusClusterSmall --archive_name desar2home
    --jira_summary 'preparation skytemplates Y5'
    --eups_stack finalcut Y4A1+5
    --queue_size 50 --calnite 20160921t1003 --calrun r3335p02
    --ignore_processed
    ```
With the results of the above, the last 2 steps of the skytemplates creation
can be achieved

### PCA calculation, per focal plane

1. First, create the list of binned focal plane images to be input to `sky_pca`
from which the 4 components will be calculated
    ```SQL
    select distinct(fai.path, fai.filename)
    from file_archive_info fai, pfw_attempt att, desfile d, exposure e
    where att.reqnum={REQNUM}
      and att.attnum=1
      and fai.filename=d.filename -- also fai.desfile_id=d.id
      and d.pfw_attempt_id=att.id
      and d.filetype='bleedmask_binned_fp'
      and CONCAT('D00', e.expnum)=att.unitname
      and e.band={BAND}
      order by fai.filename; > {bleedmask binned fp images}
    ```
1. Modify the output CSV to be used as input list
    ```bash
    awk -F "," '{print "/archive_data/desarchive/"$1"/"$2}' {CSV as above}
    ```
1. A typical call to `sky_pca` should be something like
    ```bash
    sky_pca -i {bleedmask binned fp paths}
    -o {PCA mini n04}
    -n 4
    --reject_rms {rejection value}
    -s {full filename path to save config}
    -l {full filename path to save logs}
    -v
    ```
using the following RMS values for rejection

|band | RMS reject value|
|:---:|:---------------:|
|   g | 0.008           |
|   r | 0.005           |
|   i | 0.005           |
|   z | 0.003           |
|   Y | 0.004           |

### Create the skytemplate for un-binned CCDs

### Run CCD by CCD, by hand
1. First, create a list per CCD for the reduced images to be used for the
sky template. Note use of `miscfile` table doesn't work because don't have
entries for *red_pixcor* filtype.  
    ```SQL
    select e.expnum, fai.path, fai.filename, fai.compression
    from file_archive_info fai, pfw_attempt att, desfile d, exposure e
    where att.reqnum={REQNUM}
        and att.attnum={ATTNUM}
        and fai.desfile_id=d.id
        and d.pfw_attempt_id=att.id
        and d.filetype='red_pixcor'
        and CONCAT('D00', e.expnum)=att.unitname
        and fai.filename like '%_c{0:2d CCDNUM}_%'
        and e.band={BAND}
        order by att.unitname; > {pixcor list per CCD}
    ```

1. If doing it in `bash` can use:
    ```bash
    awk -F "," '{print $1 " /archive_data/desarchive/" $2 "/" $3}' {CSV as above}
    ```

1. We have 2 options
    1. Use a table containing `expnum full_path` for all the files to be
    used ingredients. The above `awk` line helps in construct that table.
    In this case, the argument `--input_list ExpnumPath_c01_g_y5.txt` need
    to be used
    1. Copy the files and put them under the same local directory, then use
    the argument to input a template
    `pixcor_tmp/D{expnum:08d}_g_c{ccd:02d}_r3439p01_pixcor.fits`
    1. Considering the above, a typical call to *sky_template*, on a CCD
    basis would be
        ```bash
        sky_template
            --saveconfig config/skytemplate_c01_g_y5.config
            --log log/skytemplate_c01_g_y5.log
            --infile pcamini_n04_g_y5.fits
            --outfilename skytemplate_n04_c01_g_y5.fits
            --ccdnum 1
            --reject_rms 0.008
            --good_filename good_skytemplate_c01_g_y5.fits
            -v
            --input_list ExpnumPath_c01_g_y5.txt OR
            --input_template pixcor_tmp/D{expnum:08d}_g_c{ccd:02d}_rNNNNpNN_pixcor.fits
        ```

### Run using my wrapper, in parallel
1. The code **call_skytemplate.py** is a wrapper for `sky_template`. It runs
over the set of band and CCDs, using the ingredients coming from a single
pipeline run (reqnum, attnum, filetype *red_pixcor*). The process is launch
in parallel, with the ability to increase the default chunk size (1 by default)
to a larger values, to avoid memory errors. Larger (about ~100) values of
chunk size will decrease the speed but will be safer, avoiding memory issues.

1. A typical call would be
    ```bash
    python call_skytemplate.py --label Y5_withY2N_ch250 --chunk 250
    ```
For more information, display the help from *call_skytemplate.py*

## Setup of older Y2N stack
1. Due to the issue *sky_pca* outputs, using actual stacks have some strange
binning, an older must be setup.
Use the following while the issue is not solved.

    ```bash
    source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh
    setup -r /{work devel of MY USER}/git/pipebox/
    setup -r /{work devel of MY USER}/svn/desdmreportingframework/trunk/
    export DES_DB_SECTION=db-desoper
    export X509_USER_PROXY=/home/{MY USER}/.globus/osg/user.proxy
    setup -v Y2Nstack 1.0.6+14
    export HISTTIMEFORMAT="%d/%m/%y %T "
    setup -v easyaccess 1.4.2+0
    setup -v pandas 0.15.2+2
    ```
