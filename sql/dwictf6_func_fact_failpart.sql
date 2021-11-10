-- FUNCTION: dwictf6.func_fact_failpart()

-- DROP FUNCTION dwictf6.func_fact_failpart();

CREATE OR REPLACE FUNCTION dwictf6.func_fact_failpart(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
    v_func_name character varying(100) := 'func_fact_failpart';
	v_source_table character varying(100) := 'ictlogsn';
	--v_id_point integer;
	--v_max_id integer;
	_lower_testtime timestamp without time zone;
	_upper_testtime timestamp without time zone;

	v_starttime timestamp without time zone;
	v_endtime timestamp without time zone;
	v_jobname character varying (100) := v_func_name;
	v_jobtype character varying (100) := 'Data';
	v_jobsubstype character varying (100) := 'ETL';
	v_owner_str character varying (100) = 'bdcuser';   -- modify ictuser->bdcuser for M101
	v_ispass_result integer;
	v_srcbeginid bigint;
	v_srcendid bigint;
	v_srcbegintime timestamp without time zone;
	v_srcendtime timestamp without time zone;
	v_addcount integer;
	v_time_elapse numeric;
	v_cdt_time timestamp without time zone;
	v_msg character varying (500); 
	debug_time_start timestamp without time zone;
	debug_time_end timestamp without time zone;

    _now timestamp without time zone; 
    _yesterday timestamp without time zone; 

BEGIN

    set time zone 'Asia/Shanghai';  -- 设时区
    v_starttime :=clock_timestamp();
    _now := now();  
    _yesterday := now() - '120 hour'::interval; -- For fixture 与 station, pdline 关系表, 暂时取15天 -- Modify 360->120 for M101. 缩短时间, 提速.

/*1. 确定 抽取数据范围, UPPER (上限) _upper_testtime, LOWER (下限) _lower_testtime, 来源表:t_point_dw*/
    raise notice E'\n#### STEP 1. Get testtime LOWER / UPPER limit -- [%]', clock_timestamp();
	--首次运行, 直接设定起始时间为 '2019-04-01'
    if not exists(
        select datepoint from manager.t_point_dw where func_name=v_func_name and source_table = v_source_table)
	then
	    insert into manager.t_point_dw (func_name,idpoint,datepoint,source_table) 
            values (v_func_name, 0, '2019-04-01',v_source_table);  -- 时间戳 data point 下限的起点: 2019-04-01
	end if;

    -- 从ict.ictlogsn表获取当前最大 testtime , 作为导入数据的结束 testtime:
    select max(testtime) into _upper_testtime FROM ictf6.ictlogsn;    -- modify ict->ictf6 for M101
    -- 从t_point_dw 获得时间前一次的最后一笔的 testtime -1h, 作为下次取数据的起始testtime. 回退1h 是因为有可能有时间在前的数据后被解析出来:
    select datepoint - interval'1 hours'  into _lower_testtime from  manager.t_point_dw where func_name=v_func_name;  
    -- 最大: _upper_testtime
    -- 最小: _lower_testtime
    raise notice E'\nTIME Point:  LOWER: [%]  --  UPPER: [%]', _lower_testtime, _upper_testtime;

/*2. 删除 目的表 fact_failpart 中的testtime 在LOWER testtime 后的数据, 以免数据重复*/
    raise notice E'\n#### STEP 2. Delete Overlapping data -- [%]', clock_timestamp();
    delete from dwictf6.fact_failpart dest    -- modify dwict->dwictf6 for M101
        where dest.testtime >= _lower_testtime; --删除
    debug_time_start:=clock_timestamp();
    raise notice E'Start time: %', debug_time_start;

/*3. 临时表, 装入从 ictlogtestpart_ao 中, 取出当前时间段(上一次到现在)的不良明细, 解决速度问题. */
    raise notice E'\n#### STEP 3. Load logtestpart into temp table -- [%]', clock_timestamp();
    /* delete for M001 -- f3是gp DB, testtime where 条件如果用变量的话, 分区表不起作用, 查询一样慢, 必须用常数. 因此改为动态 SQL
    drop table if exists temppart;
    create temp table temppart as 
        select * from ict.ictlogtestpart_ao  
            where testtime between _lower_testtime and _upper_testtime --时间范围
                and ispass <> 1 
                and isdel <> 1 
                and model in (select distinct model from dwict.managermodel);
    */
    /*增加步骤: 为提速, 将 ict.ictlogtestpart_ao 中本次时间段的数据先放入临时表, 并且需要用execute执行查询, 原因是testtime where 条件如果用变量的话, 
            分区表不起作用, 查询一样慢, 必须用常数. 因此只能用动态sql. M001                                                                       */
--    execute 'create temp table temppart as select *, dwictf6.round_timestamp(testtime, 10) as roundtime from ictf6.ictlogtestpart_ao where testtime between '''||_lower_testtime||'''  and '''||_upper_testtime||
--             ''' and ispass <> 1 and isdel <> 1  and model in (select distinct model from dwictf6.managermodel);';
    execute 'create temp table temppart as select *, dwictf6.round_timestamp(testtime, 10) as roundtime from ictf6.ictlogtestpart_ao where testtime between '''||_lower_testtime||'''  and '''||_upper_testtime||
             ''' and ispass <> 1 and isdel <> 1;';
            -- modify ict->ictf6, dwict->dwictf6 for M101. delete  'DISTRIBUTED BY(model)' for M101
            -- add "dwictf6.round_timestamp(testtime, 10) as roundtime " for M102
            -- delete "model in (select ... from managermodel)" for M103
/*4. 临时表, 用fis pca_log fixture <=> pdline,wc 关系表. 用以填写pdline,wc */
    raise notice E'\n#### STEP 4. Create fixture vs station & pdline relational table -- [%]', clock_timestamp();
    drop table if exists fix2wcline;
    create temp table fix2wcline as
        with fixqty as(
            select distinct 
                    s.model, 
                    dwictf6.fixno2fixtureid(l.fixno) as fixture,    -- modify dwict->dwictf6 for M101.
                    l.pdline,
                    l.wc as wc, 
                    count(*) as quantity,
                    max(l.cdt) as maxcdt
                from fis.pca_pca_log l
                    inner join fis.pca_pca_sno s 
                        on l.mcbsno=s.mcbsno 
                where l.wc in ('10','12','1F')
                    and l.cdt between _yesterday and _now -- 读取fis log 的时间范围
                group by model, fixture, l.pdline, l.wc
        )
        -- quantity * (extract(epoch from maxcdt - _yesterday))/86400 是为解决 quantity的重复情况, 当出现相同的max(quantity) 时, 取时间新的.
        , fixture as(
            select model, fixture, pdline, wc, quantity * (extract(epoch from maxcdt - _yesterday))/86400 as quantity from fixqty
        )  
        select fixall.model, fixall.fixture, fixall.pdline, fixall.wc, 
                case fixall.wc when '10' then 'ICT2' when '12' then 'ICT3' when '1F' then 'CT' end as station
            from fixture fixall inner join 
                (select distinct model, fixture, max(quantity) as maxqty from fixture group by model, fixture) fixmax
                on fixall.model = fixmax.model
                    and fixall.fixture = fixmax.fixture
                    and fixall.quantity = fixmax.maxqty;
        --DISTRIBUTED BY(model); delete for M101
/*4.1 Add for M102, 临时表 testlog:  fis.pca_ict_testlog testlog  */
    raise notice E'\n#### STEP 4.1 Create temptable testlog:  fis.pca_ict_testlog testlog  -- [%]', clock_timestamp();
    drop table if exists testlog;
    create temp table testlog as
        select *,  dwictf6.round_timestamp(endtime, 10) as roundtime 
            from fis.pca_ict_testlog testlog
            where endtime between _lower_testtime and _upper_testtime
            and result='FAIL';

/*5. 插入新数据                                                    */
    -- 5.1 插入 TRI/I1000 的数据:
    raise notice E'\n#### STEP 5.1 Insert TRI/I1000 data into TABLE: fact_failpart -- [%]', clock_timestamp();
    insert into dwictf6.fact_failpart(    -- modify dwict->dwictf6 for M101.
        --FROM ictlogtestpart_ao
            mcbsno,
            family,
            model,
            fixture8,
            fixtureend,
            devicetype,
            testpart,
            location,
            remark,
            parttype,
            pin,
            testtime,
            transactionid,
            txtmeasureval,
            measureval   ,
            stdval       ,
            unit         ,
            hlim         ,
            llim         ,
            ispass       ,
            teststep     ,
            testpartnum  ,
            probe1       ,
            probe2       ,
            guard1       ,
            guard2       ,
            guard3       ,
            guard4       ,
            guard5       ,
            isdel        ,
        --FROM inclogsn
            sno,
            snoispass,
            software_rev ,
            hardware_rev ,
            logsncdt,
        --FROM fix2wcline
            pdline,
            wc,
            station,
        --FROM pca_ict_testlog
            faildesc
            )
        select distinct 
            --FROM ictlogtestpart_ao
                part.mcbsno, 
                part.family, 
                part.model, 
                part.fixture8, 
                part.fixtureend, 
                part.devicetype, 
                part.testpart, 
                part.location, 
                part.remark, 
                part.parttype,   
                --case when part.probe1=1 then array[part.probe2]  -- 加上对GND过滤和Probe1,2的排序     -- delete for M105
                --    when part.probe2=1 then array[part.probe1]                                        -- delete for M105
                case when part.probe1 IS NULL then string_to_array(testpart,',')::integer[]  -- 如果probe1 是NULL, 则将testpart专成 pin, 这是基于前面数据的观察结果: 看上去所有{NULL, NULL}的pin, 其testpart都是test pin
                    when part.probe1>part.probe2 then array[part.probe2, part.probe1]  
                    else array[part.probe1, part.probe2]
                    end as pin,
                part.testtime,
                part.transactionid,
                part.txtmeasureval,
                part.measureval,
                part.stdval,
                part.unit,
                part.hlim,
                part.llim,
                part.ispass,
                part.teststep,
                part.testpartnum ,
                part.probe1,
                part.probe2,
                part.guard1,
                part.guard2,
                part.guard3,
                part.guard4,
                part.guard5,
                part.isdel,
        --FROM ict.ictlogsn
                sn.sno,
                sn.snoispass, 
        --FROM fis.pca_ict_testlog
                testlog.softwarerev,  --software_rev 要从ict_testlog中取, ictlogsn的software_rev都是空的,2019-12-23
                testlog.hardwarerev,  --hardware_rev 要从ict_testlog中取, ictlogsn的hardware_rev都是空的,2019-12-23
                sn.cdt as logsncdt,  -- cdt 
        --FROM fix2wcline
/*[ delete for M104
                wcline.pdline as pdline,
                wcline.wc as wc, 
                wcline.station, 
]*/
--[ add for M104
                coalesce((select line from fis.pca_model_line ml where ml.model=part.model and wc in ('10','12') order by udt desc limit 1),
                         (select pdline from fix2wcline where fix2wcline.model=part.model and fix2wcline.fixture=part.fixture8||part.fixtureend limit 1) ) as pdline, 
                (select wc from fix2wcline where fix2wcline.model=part.model and fix2wcline.fixture=part.fixture8||part.fixtureend limit 1) as wc,
                (select station from fix2wcline where fix2wcline.model=part.model and fix2wcline.fixture=part.fixture8||part.fixtureend limit 1) as station,
--]
        --FROM pca_ict_testlog
                testlog.faildesc
            from temppart part--ict.ictlogtestpart_ao part
                inner join ictf6.ictlogsn sn ON     -- modify ict->ictf6 for M101
                    part.transactionid = sn.transactionid
/*[  delete for M104
                inner join fix2wcline wcline ON
                    part.model = wcline.model
                    and part.fixture8||part.fixtureend = wcline.fixture
]*/
                --left join fis.pca_ict_testlog testlog ON    --delete for M102
                left join testlog ON    --add for M102
                    --dwictf6.round_timestamp(part.testtime, 10) = dwictf6.round_timestamp(testlog.endtime, 10) --10s取整    -- modify dwict->dwictf6 for M101    --delete for M102
                    part.roundtime = testlog.roundtime    --add for M102
                    and part.fixture8||part.fixtureend = substring(testlog.fixtureid,1,8)||substring(testlog.fixtureid,length(testlog.fixtureid),1)
                    and sn.sno = testlog.barcode
            where sn.devicetype in ('I1000', 'TRI')  -- in ('I1000', 'TRI')
                and sn.snoispass <> 1
                and part.ispass <> 1
                and sn.testtime between _lower_testtime and _upper_testtime; --时间范围
                --and sn.model in (select distinct model from dwictf6.managermodel);     -- modify dwict->dwictf6 for M101   --delete for M103

    -- 5.2 插入 3070 的数据:
    raise notice E'\n#### STEP 5.2 Insert 3070 data into TABLE: fact_failpart -- [%]', clock_timestamp();

    create temp table fp3070 as(
        select distinct 
            --FROM ictlogtestpart_ao
                part.mcbsno, 
                part.family, 
                part.model, 
                part.fixture8, 
                part.fixtureend, 
                part.devicetype, 
                part.testpart, 
                part.location, 
                part.remark, 
                part.parttype, 
                part.testtime, 
                part.transactionid,
                part.txtmeasureval,
                part.measureval,
                part.stdval,
                part.unit,
                part.hlim,
                part.llim,
                part.ispass,
                part.teststep,
                part.testpartnum ,
                part.probe1,
                part.probe2,
                part.guard1,
                part.guard2,
                part.guard3,
                part.guard4,
                part.guard5,
                part.isdel,
        --FROM inclogsn
                sn.sno,
                sn.snoispass, 
        --FROM fis.pca_ict_testlog
                testlog.softwarerev,  --software_rev 要从ict_testlog中取, ictlogsn的software_rev都是空的,2019-12-23
                testlog.hardwarerev,  --hardware_rev 要从ict_testlog中取, ictlogsn的hardware_rev都是空的,2019-12-23
                sn.cdt, 
        --FROM fix2wcline
/*[ delete for M104
                wcline.pdline as pdline,
                wcline.wc as wc,
                wcline.station,
]*/
--[ add for M104
                coalesce((select line from fis.pca_model_line ml where ml.model=part.model and wc in ('10','12') order by udt desc limit 1),
                         (select pdline from fix2wcline where fix2wcline.model=part.model and fix2wcline.fixture=part.fixture8||part.fixtureend limit 1) ) as pdline, 
                (select wc from fix2wcline where fix2wcline.model=part.model and fix2wcline.fixture=part.fixture8||part.fixtureend limit 1) as wc,
                (select station from fix2wcline where fix2wcline.model=part.model and fix2wcline.fixture=part.fixture8||part.fixtureend limit 1) as station,
--]
        --FROM pca_ict_testlog
                testlog.faildesc
            from temppart part --ict.ictlogtestpart_ao part
                inner join ictf6.ictlogsn sn ON      -- modify ict->ictf6 for M101
                    part.transactionid = sn.transactionid
/*[  delete for M104
                inner join fix2wcline wcline ON
                    part.model = wcline.model
                    and part.fixture8||part.fixtureend = wcline.fixture
]*/
                --left join fis.pca_ict_testlog testlog ON    --delete for M102
                left join testlog ON    --add for M102
                    --dwictf6.round_timestamp(part.testtime, 10) = dwictf6.round_timestamp(testlog.endtime, 10) --10s取整    -- modify dwict->dwictf6 for M101   --delete for M102
                    part.roundtime = testlog.roundtime    --add for M102
                    and part.fixture8||part.fixtureend = substring(testlog.fixtureid,1,8)||substring(testlog.fixtureid,length(testlog.fixtureid),1)
                    and sn.sno = testlog.barcode
            where sn.devicetype='3070' 
                and sn.snoispass <> 1
                and part.ispass <> 1
                and sn.testtime between _lower_testtime and _upper_testtime); --时间范围
                --and sn.model in (select distinct model from dwictf6.managermodel));    --modify dwict->dwictf6 for M101     --delete for M103
    --) DISTRIBUTED BY(model);    --delete for M101

    create temp table wirelist as(
        with newbomfile as (  -- 取最新 modelbom_map 表的文件, 相同的model, wc, devicetype, filetype 取最新udt文件
            select * from(
                select row_number() over (partition by file.model, file.fixture, file.devicetype, file.filetype order by file.udt desc) as rn, * 
                    from ictf6.modelbom_map file    -- modify ict->ictf6 for M101
                    order by model, devicetype, filetype, rn
            ) allfiles
            where rn=1
        )-- 取新的 wirelist 目的是取 testpin:
        select file.family, file.model, file.fixture,  wir.testpart, wir.netname, wir.testpin, wir.partnumber, wir.bus, wir.cdt --增加 wirelist 的 bus 栏位
            from ictf6.ictwirelist wir     -- modify ict->ictf6 for M101
                inner join newbomfile file
                    on wir.bommap_uuid = file.time_uuid 
                    and wir.filename = file.filename
            order by  wir.testpin);
    --) DISTRIBUTED BY(model);     --delete for M101

    insert into dwictf6.fact_failpart(     -- modify dwict->dwictf6 for M101
        --FROM ictlogtestpart_ao
            mcbsno,
            family,
            model,
            fixture8,
            fixtureend,
            devicetype,
            testpart,
            location,
            remark,
            parttype,
            pin,
            testtime,
            transactionid,
            txtmeasureval,
            measureval   ,
            stdval       ,
            unit         ,
            hlim         ,
            llim         ,
            ispass       ,
            teststep     ,
            testpartnum  ,
            probe1       ,
            probe2       ,
            guard1       ,
            guard2       ,
            guard3       ,
            guard4       ,
            guard5       ,
            isdel        ,
        --FROM inclogsn
            sno,
            snoispass,
            software_rev ,
            hardware_rev ,
            logsncdt,
        --FROM fix2wcline
            pdline,
            wc,
            station,
        --FROM pca_ict_testlog
            faildesc
            )
        -- 将 fail part 与 wirelist join:
        select  
        --FROM ictlogtestpart_ao
                fp3070.mcbsno,
                fp3070.family,
                fp3070.model,
                fp3070.fixture8,
                fp3070.fixtureend,
                fp3070.devicetype,
                fp3070.testpart,
                fp3070.location,
                fp3070.remark, 
                fp3070.parttype,
                --(array_agg(wirelist.testpin order by wirelist.testpin))[1:2] as pin, --有些parttype(如:BS-CON )会有多于2个的pin, 只取前2个 --delete for M106
                (array_agg(wirelist.testpin order by wirelist.testpin)) as pin,        --去掉数组只取2个的限制 for M106
                fp3070.testtime,
                fp3070.transactionid,
                fp3070.txtmeasureval,
                fp3070.measureval   ,
                fp3070.stdval       ,
                fp3070.unit         ,
                fp3070.hlim         ,
                fp3070.llim         ,
                fp3070.ispass       ,
                fp3070.teststep     ,
                fp3070.testpartnum  ,
                fp3070.probe1       ,
                fp3070.probe2       ,
                fp3070.guard1       ,
                fp3070.guard2       ,
                fp3070.guard3       ,
                fp3070.guard4       ,
                fp3070.guard5       ,
                fp3070.isdel        ,
        --FROM inclogsn
                fp3070.sno,
                fp3070.snoispass,
                fp3070.softwarerev,
                fp3070.hardwarerev,
                fp3070.cdt as logsncdt,
        --FROM fix2wcline
                fp3070.pdline as pdline,
                fp3070.wc as wc,
                fp3070.station,
        --FROM pca_ict_testlog
                fp3070.faildesc
            from fp3070 
                left join wirelist 
                    on fp3070.model = wirelist.model
                    and fp3070.fixture8||fp3070.fixtureend = wirelist.fixture 
                    --and fp3070.testpart = wirelist.testpart    --delete for M108
                    and fp3070.location = wirelist.testpart      --add for M108
                    --and wirelist.netname <> 'GND'        --delete for M107
            group by fp3070.mcbsno, fp3070.family, fp3070.model,fp3070.fixture8, fp3070.fixtureend, fp3070.devicetype, fp3070.testpart, fp3070.location, fp3070.remark, fp3070.parttype, fp3070.testtime, 
                    fp3070.transactionid, fp3070.txtmeasureval, fp3070.measureval, fp3070.stdval, fp3070.unit, fp3070.hlim, fp3070.llim, fp3070.ispass, fp3070.teststep, fp3070.testpartnum, 
                    fp3070.probe1, fp3070.probe2, fp3070.guard1, fp3070.guard2, fp3070.guard3, fp3070.guard4, fp3070.guard5, fp3070.isdel,
                    fp3070.sno, fp3070.snoispass, fp3070.softwarerev, fp3070.hardwarerev, fp3070.cdt,
                    fp3070.station, fp3070.wc, fp3070.pdline,
                    fp3070.faildesc;
/*6.更新idpoint*/
    debug_time_end:=clock_timestamp();
    RAISE NOTICE E'\nComplete time: [%]. Duration: [%].', debug_time_end, extract(epoch from (debug_time_end-debug_time_start));
    update manager.t_point_dw  set datepoint = _upper_testtime where  func_name=v_func_name and source_table = v_source_table;

/*7.更新log*/
	v_endtime :=clock_timestamp();
	v_ispass_result := 1;
	v_srcbeginid := 0;
	v_srcendid := 0;	
    select count(*) into v_addcount from dwictf6.fact_failpart fact where fact.testtime between _lower_testtime and _upper_testtime;    --modify dwict->dwictf6 for M101
    raise notice 'Add Count: %', v_addcount; 
	v_srcbegintime :=_lower_testtime ;
	v_srcendtime :=_upper_testtime ;
	select extract(epoch FROM (v_endtime - v_starttime)) into v_time_elapse;
	v_cdt_time :=clock_timestamp();
	v_msg := 'success';

	PERFORM manager.insertlog(
		v_starttime,
		v_endtime,
		v_jobname,
		v_jobtype,
		v_jobsubstype,
		v_owner_str,
		v_ispass_result,
		v_srcbeginid,
		v_srcendid,
		v_srcbegintime,
		v_srcendtime,
		v_addcount,
		v_time_elapse,
		v_cdt_time,
		v_msg
	);

END;
$BODY$;

ALTER FUNCTION dwictf6.func_fact_failpart()
    OWNER TO bdcuser;
