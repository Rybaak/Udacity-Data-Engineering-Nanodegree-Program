import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']

output_data = 's3://udacity-capstone-output2/'

DEMOGRAPHICS_DATA = output_data + 'demographics_table/'
FIREARM_DATA = output_data + 'firearm_table/'
SHOOTINGS_DATA = output_data + 'shootings_table/'

staging_demographics_table_drop = "DROP TABLE IF EXISTS staging_demographics;"
staging_firearm_table_drop = "DROP TABLE IF EXISTS staging_firearm;"
staging_shootings_table_drop = "DROP TABLE IF EXISTS staging_shootings;"

analyze_staging_demographics = 'analyze staging_demographics;'
analyze_staging_firearm = 'analyze staging_firearm;'
analyze_staging_shootings = 'analyze staging_shootings;'

analyze_dim_state = 'analyze dim_state;'
analyze_dim_time = 'analyze dim_time;'
analyze_dim_demographic = 'analyze dim_demographics;'
analyze_dim_firearm_statistic = 'analyze dim_firearm_statistic;'
analyze_fact_shootings = 'analyze fact_shootings'

staging_demographics_table_create= ("""
 CREATE TABLE staging_demographics (
            city TEXT,
            state TEXT,
            median_age decimal,
            male_population INTEGER,
            female_population INTEGER,
            total_population INTEGER,
            number_of_veterans INTEGER,
            foreign_born INTEGER,
            average_household_size decimal,
            state_code TEXT,
            race TEXT,
            count INTEGER)
            DISTSTYLE ALL
            SORTKEY (state, city);
""")

staging_firearm_table_create= ("""
 CREATE TABLE staging_firearm (
            year_partition INTEGER,
            month_partition INTEGER,
            month TEXT,
            state TEXT distkey,
            permit INTEGER,
            permit_recheck INTEGER,
            handgun INTEGER,
            long_gun INTEGER,
            other INTEGER,
            multiple INTEGER,
            admin INTEGER,
            prepawn_handgun INTEGER,
            prepawn_long_gun INTEGER,
            prepawn_other INTEGER,
            redemption_handgun INTEGER,
            redemption_long_gun INTEGER,
            redemption_other INTEGER,
            returned_handgun INTEGER,
            returned_long_gun INTEGER,
            returned_other INTEGER,
            rentals_handgun INTEGER,
            rentals_long_gun INTEGER,
            private_sale_handgun INTEGER,
            private_sale_long_gun INTEGER,
            private_sale_other INTEGER,
            return_to_seller_handgun INTEGER,
            return_to_seller_long_gun INTEGER,
            return_to_seller_other INTEGER,
            totals INTEGER)
            DISTSTYLE KEY
            SORTKEY (state);
""")

staging_shootings_table_create= ("""
 CREATE TABLE staging_shootings (
            new_date TEXT,
            year_partition INTEGER,
            month_partition INTEGER,
            Date TEXT,
            City TEXT,
            State TEXT distkey,
            AreaType TEXT,
            School TEXT,
            Fatalitie TEXT,
            Wounded TEXT,
            Dupe TEXT,
            Source TEXT)
            DISTSTYLE KEY
            SORTKEY (state);
""")

staging_demographics_copy = (f"""
            copy staging_demographics 
            from '{DEMOGRAPHICS_DATA}'
            iam_role {IAM_ROLE}
            csv;
""")

staging_firearm_copy = (f"""
            copy staging_firearm 
            from '{FIREARM_DATA}'
            iam_role {IAM_ROLE}
            csv;
""")

staging_shootings_copy = (f"""
            copy staging_shootings 
            from '{SHOOTINGS_DATA}'
            iam_role {IAM_ROLE}
            FORMAT AS PARQUET;
""")

dim_state = ("""
CREATE TABLE IF NOT EXISTS dim_state
(
            ID BIGINT IDENTITY (1, 1),
            STATE VARCHAR(100),
            PRIMARY KEY (ID)
)
    DISTSTYLE ALL
    SORTKEY (state);
""")

dim_time = ("""
CREATE TABLE IF NOT EXISTS DIM_TIME
(
    DATE    DATE,
    HOUR    INTEGER,
    DAY     INTEGER,
    WEEK    INTEGER,
    MONTH   INTEGER,
    YEAR    INTEGER,
    WEEKDAY INTEGER
)
    DISTSTYLE ALL
    SORTKEY (DATE);

""")

dim_demographic = ("""
CREATE TABLE IF NOT EXISTS dim_demographics
(
            ID_STATE BIGINT,
            AVG_MEDIANA_AGE NUMERIC(38),
            SUM_MALE_POPULATION BIGINT,
            SUM_FEMALE_POPULATION BIGINT,
            SUM_TOTAL_POPULATION BIGINT,
            SUM_NUMBER_OF_VETERANS BIGINT,
            SUM_FOREIGN_BORN BIGINT,
            AVG_AVERAGE_HOUSEHOLD_SIZE NUMERIC(38),
            SUM_COUNT_WHITE BIGINT,
            SUM_COUNT_LATINO BIGINT,
            SUM_COUNT_ASIAN BIGINT,
            SUM_COUNT_BLACK BIGINT,
            SUM_COUNT_NATIVE BIGINT
)
            DISTSTYLE ALL
            SORTKEY (ID_STATE);
""")


dim_firearm_statistic = ("""
CREATE TABLE IF NOT EXISTS DIM_FIREARM_STATISTIC
(
            ID_STATE BIGINT DISTKEY,
            DATE DATE ,
            COUNT_PERMIT INTEGER ,
            COUNT_PERMIT_RECHECK INTEGER ,
            COUNT_HANDGUN INTEGER ,
            COUNT_LONG_GUN INTEGER ,
            COUNT_OTHER INTEGER ,
            COUNT_MULTIPLE INTEGER ,
            COUNT_ADMIN INTEGER ,
            COUNT_PREPAWN_HANDGUN INTEGER ,
            COUNT_PREPAWN_LONG_GUN INTEGER ,
            COUNT_PREPAWN_OTHER INTEGER ,
            COUNT_REDEMPTION_HANDGUN INTEGER ,
            COUNT_REDEMPTION_LONG_GUN INTEGER ,
            COUNT_REDEMPTION_OTHER INTEGER ,
            COUNT_RETURNED_HANDGUN INTEGER ,
            COUNT_RETURNED_LONG_GUN INTEGER ,
            COUNT_RETURNED_OTHER INTEGER ,
            COUNT_RENTALS_HANDGUN INTEGER ,
            COUNT_RENTALS_LONG_GUN INTEGER ,
            COUNT_PRIVATE_SALE_HANDGUN INTEGER ,
            COUNT_PRIVATE_SALE_LONG_GUN INTEGER ,
            COUNT_PRIVATE_SALE_OTHER INTEGER ,
            COUNT_RETURN_TO_SELLER_HANDGUN INTEGER ,
            COUNT_RETURN_TO_SELLER_LONG_GUN INTEGER ,
            COUNT_RETURN_TO_SELLER_OTHER INTEGER ,
            COUNT_TOTALS INTEGER
)
            DISTSTYLE KEY
            SORTKEY(ID_STATE);
""")

fact_shootings = ("""
CREATE TABLE IF NOT EXISTS FACT_SHOOTINGS
(
    ID_STATE            BIGINT DISTKEY,
    DATE                DATE,
    CITY                VARCHAR(256),
    AREATYPE            VARCHAR(256),
    TYPE_OF_SCHOOL      VARCHAR(17),
    COUNT_OF_FATALITIES INTEGER,
    COUNT_OF_WOUNDED    INTEGER,
    FLG_OF_TWO_SOURCES  VARCHAR(256),
    SOURCE              VARCHAR(20)
) DISTSTYLE KEY
  SORTKEY (ID_STATE);

""")



dim_state_insert = ("""
insert into dim_state (state)
with dane as (
    select state
    from staging_shootings
    union
    select state
    from staging_firearm
    union
    select state
    from staging_demographics
)
select state
from dane
where state not in (select state from dim_state);
""")
      
    
dim_time_insert = ("""
insert into dim_time
    (date, hour, day, week, month, year, weekday)
with dane as (
    select to_date(month, 'yyyy-mm') as date
    from staging_firearm
    union
    select to_date(date, 'mm/dd/yy') as date
    from staging_shootings)
select date,
       EXTRACT(hour FROM date)    AS hour,
       EXTRACT(day FROM date)     AS day,
       EXTRACT(week FROM date)    AS week,
       EXTRACT(month FROM date)   AS month,
       EXTRACT(year FROM date)    AS year,
       EXTRACT(weekday FROM date) AS weekday
from dane
where date not in (select date from dim_time);
""")

dim_demographic_insert = ("""
insert into dim_demographics (id_state,
                              avg_mediana_age,
                              sum_male_population,
                              sum_female_population,
                              sum_total_population,
                              sum_number_of_veterans,
                              sum_foreign_born,
                              avg_average_household_size,
                              sum_count_white,
                              sum_count_latino,
                              sum_count_asian,
                              sum_count_black,
                              sum_count_native)
select dstate.id,
       avg(avg_mediana_age)            avg_mediana_age,
       sum(sum_male_population)        sum_male_population,
       sum(sum_female_population)      sum_female_population,
       sum(sum_total_population)       sum_total_population,
       sum(sum_number_of_veterans)     sum_number_of_veterans,
       sum(sum_foreign_born)           sum_foreign_born,
       avg(avg_average_household_size) avg_average_household_size,
       sum(count_white)                sum_count_white,
       sum(count_latino)               sum_count_latino,
       sum(count_asian)                sum_count_asian,
       sum(count_black)                sum_count_black,
       sum(count_native)               sum_count_native
from (
         select state,
                city,
                avg(median_age)                                                          avg_mediana_age,
                sum(male_population)                                                     sum_male_population,
                sum(female_population)                                                   sum_female_population,
                sum(total_population)                                                    sum_total_population,
                sum(number_of_veterans)                                                  sum_number_of_veterans,
                sum(foreign_born)                                                        sum_foreign_born,
                avg(average_household_size)                                              avg_average_household_size,
                sum(case when race = 'White' then count end)                             count_white,
                sum(case when race = 'Hispanic or Latino' then count end)                count_latino,
                sum(case when race = 'Asian' then count end)                             count_asian,
                sum(case when race = 'Black or African-American' then count end)         count_black,
                sum(case when race = 'American Indian and Alaska Native' then count end) count_native
         from staging_demographics
         group by state, race, count, city
     ) sd
         left outer join dim_state dstate on sd.state = dstate.state
group by dstate.id;
""")

dim_firearm_statistic_insert = ("""
insert into dim_firearm_statistic (ID_STATE,
                                   date,
                                   count_permit,
                                   count_permit_recheck,
                                   count_handgun,
                                   count_long_gun,
                                   count_other,
                                   count_multiple,
                                   count_admin,
                                   count_prepawn_handgun,
                                   count_prepawn_long_gun,
                                   count_prepawn_other,
                                   count_redemption_handgun,
                                   count_redemption_long_gun,
                                   count_redemption_other,
                                   count_returned_handgun,
                                   count_returned_long_gun,
                                   count_returned_other,
                                   count_rentals_handgun,
                                   count_rentals_long_gun,
                                   count_private_sale_handgun,
                                   count_private_sale_long_gun,
                                   count_private_sale_other,
                                   count_return_to_seller_handgun,
                                   count_return_to_seller_long_gun,
                                   count_return_to_seller_other,
                                   count_totals)
select dstate.id,
       to_date(month, 'yyyy-mm') as date,
       permit                    as count_permit,
       permit_recheck            as count_permit_recheck,
       handgun                   as count_handgun,
       long_gun                  as count_long_gun,
       other                     as count_other,
       multiple                  as count_multiple,
       admin                     as count_admin,
       prepawn_handgun           as count_prepawn_handgun,
       prepawn_long_gun          as count_prepawn_long_gun,
       prepawn_other             as count_prepawn_other,
       redemption_handgun        as count_redemption_handgun,
       redemption_long_gun       as count_redemption_long_gun,
       redemption_other          as count_redemption_other,
       returned_handgun          as count_returned_handgun,
       returned_long_gun         as count_returned_long_gun,
       returned_other            as count_returned_other,
       rentals_handgun           as count_rentals_handgun,
       rentals_long_gun          as count_rentals_long_gun,
       private_sale_handgun      as count_private_sale_handgun,
       private_sale_long_gun     as count_private_sale_long_gun,
       private_sale_other        as count_private_sale_other,
       return_to_seller_handgun  as count_return_to_seller_handgun,
       return_to_seller_long_gun as count_return_to_seller_long_gun,
       return_to_seller_other    as count_return_to_seller_other,
       totals                    as count_totals
from staging_firearm sf
         left outer join dim_state dstate on sf.state = dstate.state;
""")


fact_shootings_insert = ("""
insert into FACT_SHOOTINGS(ID_STATE,
                           DATE,
                           CITY,
                           AREATYPE,
                           TYPE_OF_SCHOOL,
                           COUNT_OF_FATALITIES,
                           COUNT_OF_WOUNDED,
                           FLG_OF_TWO_SOURCES,
                           SOURCE)
select dstate.id,
       to_date(date, 'yyyy-mm-dd') as date,
       ss.city,
       ss.areatype,
       case
           when ss.school = 'C' then 'College'
           when ss.school = 'HS' then 'High school'
           when ss.school = 'MS' then 'Middle school'
           when ss.school = 'ES' then 'Rlementary school'
           when ss.school = '-' then 'Unknown'
           end                     as type_of_school,
       ss.fatalitie::integer       as count_of_fatalities,
       ss.wounded::integer         as count_of_wounded,
       ss.dupe                     as flg_of_two_sources,
       case
           when ss.source = 'Wikp' then 'Wikipedia article'
           when ss.source = 'Pah' then 'Adam R. Pah research'
           end                     as source
from staging_shootings ss
         left outer join dim_state dstate on ss.state = dstate.state
;
""")


data_count_quality_check = ("""
select 'dim_demographics' as table_name, count(*)
from dim_demographics
union
select 'dim_firearm_statistic', count(*)
from dim_firearm_statistic
union
select 'dim_state', count(*)
from dim_state
union
select 'dim_time', count(*)
from dim_time
union
select 'fact_shootings', count(*)
from fact_shootings order by 1 desc;
""")

data_state_unknown_quality_check = ("""
select id_state, count(*)
from FACT_SHOOTINGS
where id_state not in (select id_state from dim_state)
group by id_state
UNION
select id_state, count(*)
from dim_demographics
where id_state not in (select id_state from dim_state)
group by id_state
UNION
select id_state, count(*)
from dim_firearm_statistic
where id_state not in (select id_state from dim_state)
group by id_state
order by 2 desc
;
""")



# QUERY LISTS

drop_table_queries = [staging_demographics_table_drop, staging_firearm_table_drop, staging_shootings_table_drop]

create_table_queries = [staging_demographics_table_create, staging_firearm_table_create, staging_shootings_table_create]

copy_table_queries = [staging_demographics_copy, staging_firearm_copy, staging_shootings_copy]

analyze_stage_queries = [analyze_staging_demographics, analyze_staging_firearm, analyze_staging_shootings]

create_dim_facts_table_queries = [dim_state, dim_demographic, dim_firearm_statistic, dim_time, fact_shootings]

insert_dim_facts_table_queries = [dim_state_insert, dim_time_insert, dim_demographic_insert, dim_firearm_statistic_insert, fact_shootings_insert]

analyze_dim_facts_queries = [analyze_dim_state, analyze_dim_time, analyze_dim_demographic, analyze_dim_firearm_statistic, analyze_fact_shootings]