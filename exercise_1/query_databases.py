import sqlite3
import duckdb
import os
import inspect
import time
import pandas

SCRIPT_PATH =  os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(SCRIPT_PATH)

ncvoters_pandas = pandas.read_csv('ncvoter_sample.tsv', sep='\t', low_memory=False, names=["county_id" , "county_desc" , "voter_reg_num" , "status_cd" , "voter_status_desc" , "reason_cd" , "voter_status_reason_desc" , "absent_ind" , "name_prefx_cd" , "last_name" , "first_name" , "midl_name" , "name_sufx_cd" , "full_name_rep" , "full_name_mail" , "house_num" , "half_code" , "street_dir" , "street_name" , "street_type_cd" , "street_sufx_cd" , "unit_designator" , "unit_num" , "res_city_desc" , "state_cd" , "zip_code" , "res_street_address" , "res_city_state_zip" , "mail_addr1" , "mail_addr2" , "mail_addr3" , "mail_addr4" , "mail_city" , "mail_state" , "mail_zipcode" , "mail_city_state_zip" , "area_cd" , "phone_num" , "full_phone_number" , "drivers_lic" , "race_code" , "race_desc" , "ethnic_code" , "ethnic_desc" , "party_cd" , "party_desc" , "sex_code" , "sex" , "birth_age" , "birth_place" , "registr_dt" , "precinct_abbrv" , "precinct" , "municipality_abbrv" , "municipality_desc" , "ward_abbrv" , "ward_desc" , "cong_dist_abbrv" , "cong_dist_desc" , "super_court_abbrv" , "super_court_desc" , "judic_dist_abbrv" , "judic_dist_desc" , "nc_senate_abbrv" , "nc_senate_desc" , "nc_house_abbrv" , "nc_house_desc" , "county_commiss_abbrv" , "county_commiss_desc" , "township_abbrv" , "township_desc" , "school_dist_abbrv" , "school_dist_desc" , "fire_dist_abbrv" , "fire_dist_desc" , "water_dist_abbrv" , "water_dist_desc" , "sewer_dist_abbrv" , "sewer_dist_desc" , "sanit_dist_abbrv" , "sanit_dist_desc" , "rescue_dist_abbrv" , "rescue_dist_desc" , "munic_dist_abbrv" , "munic_dist_desc" , "dist_1_abbrv" , "dist_1_desc" , "dist_2_abbrv" , "dist_2_desc" , "confidential_ind" , "age" , "ncid" , "vtd_abbrv" , "vtd_desc"])
precinctvotes_pandas = pandas.read_csv('precinctvotes.tsv', sep='\t', low_memory=False, names = ["county", "precinct", "total_votes", "romney_percentage"])


CREATE_NCVOTERS_SQL = '''
    CREATE TABLE IF NOT EXISTS ncvoters(county_id STRING, county_desc STRING, voter_reg_num STRING,status_cd STRING, voter_status_desc STRING, reason_cd STRING, voter_status_reason_desc STRING, absent_ind STRING, name_prefx_cd STRING,last_name STRING, first_name STRING, midl_name STRING, name_sufx_cd STRING, full_name_rep STRING,full_name_mail STRING, house_num STRING, half_code STRING, street_dir STRING, street_name STRING, street_type_cd STRING, street_sufx_cd STRING, unit_designator STRING, unit_num STRING, res_city_desc STRING,state_cd STRING, zip_code STRING, res_street_address STRING, res_city_state_zip STRING, mail_addr1 STRING, mail_addr2 STRING, mail_addr3 STRING, mail_addr4 STRING, mail_city STRING, mail_state STRING, mail_zipcode STRING, mail_city_state_zip STRING, area_cd STRING, phone_num STRING, full_phone_number STRING, drivers_lic STRING, race_code STRING, race_desc STRING, ethnic_code STRING, ethnic_desc STRING, party_cd STRING, party_desc STRING, sex_code STRING, sex STRING, birth_age INTEGER, birth_place STRING, registr_dt STRING, precinct_abbrv STRING, precinct_desc STRING,municipality_abbrv STRING, municipality_desc STRING, ward_abbrv STRING, ward_desc STRING, cong_dist_abbrv STRING, cong_dist_desc STRING, super_court_abbrv STRING, super_court_desc STRING, judic_dist_abbrv STRING, judic_dist_desc STRING, nc_senate_abbrv STRING, nc_senate_desc STRING, nc_house_abbrv STRING, nc_house_desc STRING,county_commiss_abbrv STRING, county_commiss_desc STRING, township_abbrv STRING, township_desc STRING,school_dist_abbrv STRING, school_dist_desc STRING, fire_dist_abbrv STRING, fire_dist_desc STRING, water_dist_abbrv STRING, water_dist_desc STRING, sewer_dist_abbrv STRING, sewer_dist_desc STRING, sanit_dist_abbrv STRING, sanit_dist_desc STRING, rescue_dist_abbrv STRING, rescue_dist_desc STRING, munic_dist_abbrv STRING, munic_dist_desc STRING, dist_1_abbrv STRING, dist_1_desc STRING, dist_2_abbrv STRING, dist_2_desc STRING, confidential_ind STRING, age STRING, ncid STRING, vtd_abbrv STRING, vtd_desc STRING);
'''

CREATE_PRECINCTVOTES_SQL = '''
CREATE TABLE IF NOT EXISTS precinct_votes(county STRING, precinct STRING, total_votes INT, romney_percentage DOUBLE);
'''


def load_sqlite():
	if os.path.isfile("voters_sqlite.db"):
		os.system("rm voters_sqlite.db")
	db = sqlite3.connect('voters_sqlite.db')
	cursor = db.cursor()
	cursor.execute(CREATE_NCVOTERS_SQL)
	cursor.execute(CREATE_PRECINCTVOTES_SQL)
	db.commit()
	db.close()
	os.system("sqlite3 voters_sqlite.db < sqlite_import_tsv.sql")


def load_duckdb():
	if os.path.isfile("voters_duck.db"):
		os.system("rm voters_duck.db")
	db = duckdb.connect('voters_duck.db')
	cursor = db.cursor()
	cursor.execute(CREATE_NCVOTERS_SQL)
	cursor.execute(CREATE_PRECINCTVOTES_SQL)
	cursor.execute("COPY ncvoters FROM 'ncvoter_sample.tsv' DELIMITER '\t'")
	cursor.execute("COPY precinct_votes FROM 'precinctvotes.tsv' DELIMITER '\t'")
	return cursor

load_sqlite()
sqlite_db = sqlite3.connect('voters_sqlite.db')
sqlite_cursor = sqlite_db.cursor()

duck_cursor = load_duckdb()

query_01 = ''' SELECT COUNT(*) FROM ncvoters WHERE ncvoters.status_cd='A'  '''
query_02 = ''' SELECT birth_age,COUNT(*) FROM ncvoters GROUP BY birth_age HAVING birth_age =70  '''

query_03 = '''
SELECT  county_id, romney_percentage,count(*) as total_white
FROM precinct_votes
INNER JOIN ncvoters
ON ncvoters.precinct_desc=precinct_votes.precinct AND ncvoters.county_desc=precinct_votes.county
WHERE ncvoters.status_cd='A' and sex_code = 'M' and race_code = 'W' and birth_age > 40 group by county_id
order by  total_white desc Limit 3'''

query_03_duck = '''
SELECT county_id, first(romney_percentage),count(*) as total_white
FROM precinct_votes
INNER JOIN ncvoters
ON ncvoters.precinct_desc=precinct_votes.precinct AND ncvoters.county_desc=precinct_votes.county
WHERE ncvoters.status_cd='A' and sex_code = 'M' and race_code = 'W' and birth_age > 40 group by county_id
order by  total_white desc Limit 3'''

query_04a = '''DELETE FROM ncvoters WHERE birth_age < 18 OR birth_age > 120'''
query_04b = query_01
def query_01_pandas_df(df):
	pandas_query = df[["county_id"]][(df.status_cd == 'A')]
	result = (pandas_query.county_id).count()
	print (result)

def query_01_pandas():
	query_01_pandas_df(ncvoters_pandas)

def query_01_sqlite():
	sqlite_cursor.execute(query_01)
	result = sqlite_cursor.fetchone()
	print(result)

def query_01_duckdb():
	duck_cursor.execute(query_01)
	result = duck_cursor.fetchone()
	print(result)

def query_02_pandas():
	result = ncvoters_pandas.groupby('birth_age').agg({'county_id': 'count'})
	print(result[result.county_id.index == 70])

def query_02_sqlite():
	sqlite_cursor.execute(query_02)
	result = sqlite_cursor.fetchone()
	print(result)

def query_02_duckdb():
	duck_cursor.execute(query_02)
	result = duck_cursor.fetchone()
	print(result)

WHERE ncvoters.status_cd=‘A' AND sex_code = 'M'
  AND race_code = ‘W' AND birth_age > 40

def query_03_pandas():
voter_status = ncvoters_pandas['status_cd'] == 'A'
race_code = ncvoters_pandas['race_code'] == 'W'
birth_age = ncvoters_pandas['birth_age'] > 40
sex_code = ncvoters_pandas['sex_code'] = 'M'
ncvoters_pandas[voter_status & race_code & birth_age & sex_code].join(precinctvotes_pandas, on="precinct")[]

	return
	# result = ncvoters_pandas.groupby(['county_id','race_code']).agg({'county_desc': 'count'})
	# print(result[result.race_code.index == 'B' > result.race_code.index == 'W'])

def query_03_sqlite():
	sqlite_cursor.execute(query_03)
	result = sqlite_cursor.fetchall()
	print(result)

def query_03_duckdb():
	duck_cursor.execute(query_03_duck)
	result = duck_cursor.fetchall()
	print(result)

def query_04_pandas():
	birth_age_gt_18 = ncvoters_pandas['birth_age'] >= 18
	birth_age_lt_120 =  ncvoters_pandas['birth_age'] <= 120
	result = ncvoters_pandas[birth_age_gt_18 & birth_age_lt_120]
	result.to_csv('result.tsv.tmp', sep='\t')
	query_01_pandas_df(result)

def query_04_sqlite():
	sqlite_cursor.execute(query_04a)
	sqlite_cursor.execute(query_04b)
	result = sqlite_cursor.fetchone()
	print(result)

def query_04_duckdb():
	duck_cursor.execute(query_04a)
	duck_cursor.execute(query_04b)
	result = duck_cursor.fetchone()
	print(result)

start = time.time()
query_03_pandas()
end = time.time()
print("Query in pandas : " +str(end - start))

start = time.time()
query_03_sqlite()
end = time.time()
print("Query in sqlite : " +str(end - start))

start = time.time()
query_03_duckdb()
end = time.time()
print("Query in duckdb : " +str(end - start))

if os.path.isfile('result.tsv.tmp'):
	os.system("rm result.tsv.tmp")