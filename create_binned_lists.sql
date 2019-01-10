-- Query to get the path and filename of the binned focal plane, used as
-- ingredient for the PCA creation

-- band  reqnum
-- g     4005
-- r     4004
-- i     4006
-- z     4007
-- Y     4008

select fai.path, fai.filename
    from file_archive_info fai, pfw_attempt att, desfile d, exposure e
    where att.reqnum=4008
    and att.attnum=1
    and fai.filename=d.filename
    and d.pfw_attempt_id=att.id
    and d.filetype='bleedmask_binned_fp'
    and CONCAT('D00', e.expnum)=att.unitname
    and e.band='Y'
    group by fai.path, fai.filename
    order by fai.filename; > bleedmask_Y.csv

-- also fai.desfile_id=d.id
