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

1. A typical call would be
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
    select fai.path, fai.filename
    from file_archive_info fai, pfw_attempt att, desfile d, exposure e
    where att.reqnum={REQNUM}
      and att.attnum=1
      and fai.filename=d.filename
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

band | RMS reject value
=====|=================
   g | 0.008
   r | 0.005
   i | 0.005
   z | 0.003
   Y | 0.004

### Create the skytemplate for unbinned CCDs
