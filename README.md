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
    1. pixcorrect
    1. mkbleedmask
    1. skycompress-mkbleedmask
