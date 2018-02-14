# skytemplates
==============
## Routines for skytemplates generation
----------------------------------------

1. Get range of nights to work on (given by **epoch** range)

1. Select object images with typical exposure time.
    ```SQL
       with w as (select expnum, max(lastchanged_time) maxtime from finalcut_eval 
                  where analyst!='SNQUALITY' group by expnum) 
       select e.expnum, e.nite, e.exptime, e.band, e.object, fcut.t_eff 
       from w, exposure e, finalcut_eval fcut 
       where nite between NIGHT_START and NIGHT_END 
          and exptime between 20 and 100 
          and e.object like '%hex%' 
          and e.band='g' 
          and e.expnum=fcut.expnum 
          and fcut.accepted='True' 
          and fcut.program='survey' 
          and fcut.analyst != 'SNQUALITY' 
          and fcut.lastchanged_time=w.maxtime 
          and fcut.t_eff > 0.9 
          order by fcut.t_eff; > explist_{year}_{epoch}_{band}.csv
   ```

1. Run the following steps
    1. crosstalk-sci
    1. pixcorrect (save outputs)
    1. skycompress-mkbleedmask
    1. skycombine-mkbleedmask (save outputs)
    * **Note** as we are stacking many exposures, make bleed masks can be avoided, just as a statistic consequence.

1. I modified a template for `widefield` to only run the above steps. As these steps assume `mkbleedmask` ran, I modified the *wcl* files to use `pixcorrect` outputs. The `bleedmask-mini` are not specifically generated from *bleedmasked* but from *pixcorrected*
