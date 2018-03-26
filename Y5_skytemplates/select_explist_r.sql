-- Select exposures from Y5, g-band
-- T_EFF down from 0.9 to reach 600 exposures

-- Need to apply a cut around lightbulb 20171110

with w as (select expnum, max(lastchanged_time) maxtime
           from firstcut_eval
           where analyst!='SNQUALITY' group by expnum)
select e.expnum, e.nite, e.exptime, e.band, e.object, fcut.t_eff
from w, exposure e, firstcut_eval fcut
where e.nite between 20170815 and 20171109
   and e.exptime between 20 and 100
   and e.object like '%hex%'
   and e.band='r'
   and e.expnum=fcut.expnum
   and fcut.accepted='True'
   and fcut.program='survey'
   and fcut.analyst != 'SNQUALITY'
   and fcut.lastchanged_time=w.maxtime
   and fcut.t_eff > 0.79
   order by fcut.t_eff; > preSel_r_y5.csv
