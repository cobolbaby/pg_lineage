package main

import (
	"fmt"
	"os"

	"pg_lineage/internal/erd"
	"pg_lineage/pkg/config"
	"pg_lineage/pkg/log"
)

func init() {
	if err := log.InitLogger(&config.LogConfig{
		Level: "debug",
		Path:  "./logs/lineage.log",
	}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {

	sql := `
	with dimdate_month as (
		select d.datename,to_char(d1.monthfirstday,'yyyy-mm') as sortdate
		,d.monthname||'-'||d.yearname as daterange
		,to_char(d1.monthfirstday,'yyyy-mm-dd')  as sdt
		,to_char(d1.monthlastday,'yyyy-mm-dd') as edt
		from dw.dim_date_mon d
		join 
		(
			select year,monthnumber,to_date(min(datename),'yyyy-mm-dd') as monthfirstday,to_date(max(datename),'yyyy-mm-dd') as monthlastday 
			from dw.dim_date_mon group by year,monthnumber
		) d1 on d.year=d1.year and d.monthnumber=d1.monthnumber
		where d.datealternatekey >=date_trunc('year',('2024-04-21T07:00:00Z' at time zone 'Asia/Shanghai'))
	),dimdate_week as (
		select d.datename,d.weekinyear||'-'||d.weekinmonth||'-'||d.weeknumberofmonth as sortdate
		,d.weekinmonthname||'-'||d.weeknameofmonth as daterange
		,to_char(d1.weekfirstday,'yyyy-mm-dd') as sdt
		,to_char(d1.weeklastday,'yyyy-mm-dd') as edt
		from dw.dim_date_mon d
		join 
		(
		select weekinyear,weekinmonth,weeknumberofmonth,to_date(min(datename),'yyyy-mm-dd') as weekfirstday,to_date(max(datename),'yyyy-mm-dd') as weeklastday 
		from dw.dim_date_mon group by weekinyear,weekinmonth,weeknumberofmonth
		) d1 on d.weekinyear=d1.weekinyear and d.weekinmonth=d1.weekinmonth and d.weeknumberofmonth=d1.weeknumberofmonth
		where d.datealternatekey>=date_trunc('year',('2024-04-21T07:00:00Z' at time zone 'Asia/Shanghai'))
	),dimdate_day as (
		select datename ,datename  as sortdate,substring(datename,6,2)||'/'||substring(datename,9,2) as daterange	
		,to_char(datealternatekey,'yyyy-mm-dd') as sdt
		,to_char(datealternatekey,'yyyy-mm-dd') as edt
		from dw.dim_date_mon 
		where datealternatekey>=date_trunc('year',('2024-04-21T07:00:00Z' at time zone 'Asia/Shanghai'))
	),dimdate as (
		select d.datename,m.daterange as date_month,w.daterange as date_week, d.daterange as date_day,m.sdt as month_sdt,m.edt as month_edt,w.sdt as week_sdt,w.edt as week_edt,d.sdt as day_sdt,d.edt as day_edt
		from dimdate_month m
		join dimdate_week w on m.datename = w.datename
		join dimdate_day d on w.datename = d.datename
		order by datename
	),raw_date as (
		select s.test_date,s.sno,s.station,s.first_pass,s.first_fail
		from dw.fact_cpu_yield_unit s	
		join dw.fact_cpu_sn m on s.sno=m.sno 
		join dw.dim_station l on s.wc = l.wc
		join dw.dim_cpu_model d on s.model = d.model
		where s.first_pass+s.first_fail>0
		and s.test_date between ('2024-04-21T07:00:00Z' at time zone 'Asia/Shanghai') and ('2024-04-22T06:59:59Z' at time zone 'Asia/Shanghai')
		and case when 'HP' = 'HPE' then  m.customer= 'HP' else m.customer = any (string_to_array('HP',',')) end
		and case when 'ACT,PreTest,Run-in Test' = 'All' then 1=1 else s.station = any (string_to_array('ACT,PreTest,Run-in Test',',')) end
		and case when 'All' = 'All' then 1=1 else s.family = any (string_to_array('All',',')) end
		and case when 'All' = 'All' then 1=1 else s.model =  'All' end
		and case when 'All' = 'All' then 1=1 else s.pdline = any (string_to_array('All',',')) end
		and case
			when 'MP' = 'All' then 1=1
			when 'MP' = 'MP' then substring(s.workorder,1,1)='1' 
		else substring(s.workorder,1,1)<>'1' end 
		and s.sno not in (select value  from  manager.product_category_result where field= 'sno' )
		and case when 'All' = 'All' then 1=1 else build_type = 'All' end
		and d.family <> '3PAR_HDD_TEST OPTION'
	)
	select max(d.datename) as datename,
	case 
	  when 'week' = 'month' then date_month
	  when 'week' = 'week' then date_week
	  when 'week' = 'day' then date_day
	end as daterange,
	case 
	  when 'week' = 'month' then extract(epoch from to_timestamp(month_sdt,'yyyy-mm-dd'))*1000
	  when 'week' = 'week' then extract(epoch from to_timestamp(week_sdt,'yyyy-mm-dd'))*1000
	  when 'week' = 'day' then extract(epoch from to_timestamp(day_sdt,'yyyy-mm-dd'))*1000
	end as datesdt,
	case 
	  when 'week' = 'month' then extract(epoch from to_timestamp(month_edt,'yyyy-mm-dd'))*1000
	  when 'week' = 'week' then extract(epoch from to_timestamp(week_edt,'yyyy-mm-dd'))*1000
	  when 'week' = 'day' then extract(epoch from to_timestamp(day_edt,'yyyy-mm-dd'))*1000
	end as dateedt,
	count(distinct t.sno) as "Input",count(distinct t.sno) filter (where first_fail=1) as "Fail",
	round((1-count(distinct t.sno) filter (where first_fail=1)*1.0/count(distinct t.sno) ),4)::double precision as "FPY"
	from raw_date t
	join dimdate d on to_char(t.test_date,'yyyy-mm-dd') = d.datename
	group by daterange,datesdt,dateedt
	order by datename
	`

	var m map[string]*erd.RelationShip

	m, _ = erd.Parse(sql)

	n := make(map[string]*erd.RelationShip)
	for kk, vv := range m {
		// 过滤掉临时表
		if vv.SColumn == nil || vv.TColumn == nil || vv.SColumn.Schema == "" || vv.TColumn.Schema == "" {
			continue
		}
		n[kk] = vv
	}
	log.Debugf("GetRelationShip: #%d\n", len(n))
	for _, v := range n {
		log.Debugf("%s\n", v.ToString())
	}

}
