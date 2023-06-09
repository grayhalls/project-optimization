import psycopg2 
import pandas as pd
from dotenv import load_dotenv
import os 

HOST = os.getenv("POSTGRES_HOST")
DB = os.getenv("POSTGRES_DB")
PORT = os.getenv("POSTGRES_PORT")
USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")


def get_sql_connection():
    conn = psycopg2.connect(
        host=HOST,
        database=DB,
        port=PORT,
        user=USER,
        password=PASSWORD
    )

    return conn 

def run_sql_query(sql_query):
    conn = get_sql_connection()
    df = pd.read_sql_query(sql_query, conn)
    conn.close()
    
    return df 

facilities_sql = '''
with acquisition_dates as (
	select *
	from (
			values ('RD001',	'2013-03-26'),('RD002',	'2013-03-28'),
('RD003',	'2013-12-17'),('RD004',	'2013-12-17'),('RD005',	'2014-05-13'),('RD006',	'2014-03-18'),('RD007',	'2014-04-30'),('RD008',	'2014-07-24'),('RD009',	'2014-07-28'),('RD010',	'2014-08-31'),('RD011',	'2014-09-30'),
('RD012',	'2014-10-31'),('RD013',	'2014-11-17'),('RD014',	'2014-11-17'),('RD015',	'2015-02-27'),('RD016',	'2015-02-27'),('RD017',	'2015-02-27'),('RD018',	'2015-02-27'),('RD019',	'2015-02-27'),
('RD020',	'2015-01-26'),('RD021',	'2015-03-16'),('RD022',	'2015-03-16'),('RD023',	'2015-05-08'),('RD024',	'2015-08-24'),('RD025',	'2015-08-24'),('RD026',	'2015-08-24'),('RD027',	'2015-08-28'),('RD028',	'2015-09-10'),
('RD029',	'2015-10-21'),('RD030',	'2015-10-21'),('RD031',	'2015-10-21'),('RD032',	'2015-11-13'),('RD033',	'2015-10-08'),('RD034',	'2015-10-08'),('RD035',	'2015-09-30'),('RD036',	'2016-02-26'),
('RD037',	'2015-12-18'),('RD038',	'2016-01-11'),('RD039',	'2016-03-18'),('RD040',	'2016-03-18'),('RD041',	'2016-03-07'),('RD042',	'2016-03-14'),('RD043',	'2016-03-23'),('RD044',	'2016-04-13'),('RD045',	'2016-05-26'),
('RD046',	'2016-06-07'),('RD047',	'2016-06-07'),('RD048',	'2016-06-16'),('RD049',	'2016-06-22'),('RD050',	'2016-06-23'),('RD051',	'2016-06-23'),('RD052',	'2016-06-23'),('RD053',	'2016-06-23'),('RD054',	'2016-06-23'),
('RD055',	'2017-01-26'),('RD056',	'2017-01-26'),('RD057',	'2016-09-22'),('RD058',	'2016-10-07'),('RD059',	'2016-10-13'),('RD060',	'2016-10-27'),('RD061',	'2016-10-27'),('RD062',	'2016-11-15'),('RD063',	'2016-11-30'),
('RD064',	'2016-11-30'),('RD065',	'2017-01-06'),('RD066',	'2017-03-09'),('RD067',	'2016-12-28'),('RD068',	'2016-12-28'),('RD069',	'2016-12-28'),('RD070',	'2016-12-28'),('RD071',	'2017-01-31'),('RD072',	'2017-04-04'),
('RD073',	'2017-04-07'),('RD074',	'2017-04-25'),('RD075',	'2017-05-11'),('RD076',	'2017-05-11'),('RD077',	'2017-05-11'),('RD078',	'2017-06-21'),('RD079',	'2017-04-27'),('RD080',	'2017-04-27'),('RD081',	'2017-05-09'),
('RD082',	'2017-05-24'),('RD083',	'2017-05-31'),('RD084',	'2017-05-31'),('RD085',	'2017-05-31'),('RD086',	'2017-06-30'),('RD087',	'2017-09-29'),('RD088',	'2017-07-19'),('RD089',	'2017-08-16'),('RD090',	'2017-09-12'),
('RD091',	'2017-08-23'),('RD092',	'2017-08-23'),('RD093',	'2017-08-23'),('RD094',	'2018-03-23'),('RD095',	'2018-09-20'),('RD096',	'2017-12-20'),('RD097',	'2018-02-01'),('RD098',	'2018-05-24'),('RD099',	'2017-08-24'),
('RD100',	'2017-09-21'),('RD101',	'2017-09-21'),('RD102',	'2017-09-21'),('RD103',	'2018-03-21'),('RD104',	'2018-05-17'),('RD105',	'2018-04-12'),('RD106',	'2018-04-05'),('RD107',	'2018-03-08'),('RD108',	'2018-04-25'),('RD109',	'2018-06-26'),('RD110',	'2018-08-01'),('RD111',	'2018-06-05'),
('RD112',	'2018-04-24'),('RD113',	'2018-05-23'),('RD114',	'2018-06-26'),('RD115',	'2018-10-03'),('RD116',	'2018-10-03'),('RD117',	'2018-10-03'),('RD118',	'2018-10-03'),('RD119',	'2018-10-03'),('RD120',	'2018-10-03'),('RD121',	'2018-10-03'),
('RD122',	'2018-06-26'),('RD123',	'2018-06-26'),('RD124',	'2018-08-23'),('RD125',	'2018-11-27'),('RD126',	'2018-10-11'),('RD127',	'2018-10-11'),('RD128',	'2018-10-10'),('RD129',	'2018-11-06'),('RD130',	'2018-10-16'),
('RD131',	'2019-01-18'),('RD132',	'2018-10-30'),('RD133',	'2018-11-29'),('RD134',	'2018-11-07'),('RD135',	'2018-11-08'),('RD136',	'2018-12-18'),('RD137',	'2019-03-05'),('RD138',	'2019-01-29'),('RD139',	'2018-11-15'),
('RD140',	'2018-12-13'),('RD141',	'2019-02-26'),('RD142',	'2018-12-20'),('RD143',	'2019-02-14'),('RD144',	'2019-03-07'),('RD145',	'2019-02-27'),('RD146',	'2019-02-20'),('RD147',	'2019-02-21'),('RD148',	'2019-02-28'),
('RD149',	'2019-06-06'),('RD150',	'2019-04-02'),('RD151',	'2019-04-30'),('RD152',	'2019-05-03'),('RD153',	'2019-04-18'),('RD154',	'2019-06-19'),('RD155',	'2019-07-17'),('RD156',	'2020-08-18'),('RD157',	'2019-05-29'),
('RD158',	'2019-05-02'),('RD159',	'2019-05-09'),('RD160',	'2019-04-16'),('RD161',	'2019-05-07'),('RD162',	'2019-05-16'),('RD163',	'2019-05-16'),('RD164',	'2019-09-30'),('RD165',	'2019-05-14'),('RD166',	'2019-05-14'),('RD167',	'2019-05-15'),('RD168',	'2019-05-21'),
('RD169',	'2019-08-14'),('RD170',	'2019-09-26'),('RD171',	'2019-06-18'),('RD172',	'2019-06-26'),('RD173',	'2019-08-08'),('RD174',	'2019-10-29'),('RD175',	'2019-09-04'),('RD176',	'2019-08-30'),('RD177',	'2019-09-06'),('RD178',	'2020-07-23'),
('RD179',	'2020-01-29'),('RD180',	'2019-10-23'),('RD181',	'2019-10-23'),('RD182',	'2020-02-27'),('RD183',	'2020-02-27'),('RD184',	'2020-03-19'),('RD185',	'2019-11-19'),('RD186',	'2020-04-21'),('RD187',	'2020-05-27'),('RD188',	'2020-05-27'),('RD189',	'2020-06-04'),
('RD190',	'2020-10-30'),('RD191',	'2021-07-07'),('RD192',	'2020-12-29'),('RD193',	'2020-09-03'),('RD195',	'2021-12-30'),('RD196',	'2021-12-28'),('RD198',	'2021-03-11')
	)
		as ad (site_code, acq_date)
union 
select f.site_code , (f.created_at::date)::text  as acq_date 
from facilities f
where f.created_at >= '2022-01-01'
)
, supervisors as (
    select f.site_code,
           u.first_name || ' ' || u.last_name as name
    from facilities f
             left join roles r on r.resource_id = f.id and lower(r.resource_type) = 'facility'
             left join users_roles ur on r.id = ur.role_id
             left join users u on u.id = ur.user_id
    where r.name = 'supervisor'
)
select 
	f.id as facility_id,
	f.site_code as rd,
	coalesce(ad.acq_date::date, f.created_at::date) as acq_date,
	r.name as region,
	f.region_id ,
	case when f.fund = 0 then 'FAM1' when f.fund =1 then 'FAM2' when f.fund =2 then 'FAM3' when f.fund =3 then 'FAM4'
		when f.fund =4 then 'Inland' when f.fund =5 then 'RDH II' when f.fund =6 then 'RDH III' when f.fund =7 then 'RDH IV'
		when f.fund =8 then 'SPH' 
		when f.fund =9 then 'FAM5' end as fund ,
	s.name as fs,
	f.created_at as acquisition_date,
	date_part('day'::text, now() - f.created_at::timestamp with time zone) as age_of_facility,
	a.street,
	a.street_2 ,
	a.city,
	a.state,
	a.zip ,
    a.lat as latitude,
    a.lng as longitude,
    sum(u.width*u.length) as nrsf ,
    case when ad.acq_date is not null then 
    	case when ad.acq_date::date <= (now()-interval '24 months') 
    		then true else false end 
    	else case when f.created_at::date <= (now()-interval '24 months') 
    		then true else false end 
    	end as same_store 
from facilities f 
		left join addresses a on a.addressable_id = f.id and a.addressable_type::text ='Facility'::text 
		left join supervisors s on s.site_code = f.site_code
		left join acquisition_dates ad on ad.site_code = f.site_code 
		left join regions r on r.id = f.region_id 
		inner join units u on u.facility_id = f.id and u.inactive = false 
where f.id != 48 
group by f.site_code, f.id, s.name, acquisition_date , age_of_facility , a.street, a.street_2 , a.city, a.state, a.zip, latitude, longitude, r.name, f.fund, ad.acq_date 
order by f.site_code ;
'''