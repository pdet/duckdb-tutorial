import sqlite3
import duckdb
import os
import inspect
import time
import pandas

SCRIPT_PATH =  os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(SCRIPT_PATH)

ncvoters_pandas = pandas.read_csv('ncvoter_sample.tsv', sep='\t', low_memory=False, names=["county_id" , "county_desc" , "voter_reg_num" , "status_cd" , "voter_status_desc" , "reason_cd" , "voter_status_reason_desc" , "absent_ind" , "name_prefx_cd" , "last_name" , "first_name" , "midl_name" , "name_sufx_cd" , "full_name_rep" , "full_name_mail" , "house_num" , "half_code" , "street_dir" , "street_name" , "street_type_cd" , "street_sufx_cd" , "unit_designator" , "unit_num" , "res_city_desc" , "state_cd" , "zip_code" , "res_street_address" , "res_city_state_zip" , "mail_addr1" , "mail_addr2" , "mail_addr3" , "mail_addr4" , "mail_city" , "mail_state" , "mail_zipcode" , "mail_city_state_zip" , "area_cd" , "phone_num" , "full_phone_number" , "drivers_lic" , "race_code" , "race_desc" , "ethnic_code" , "ethnic_desc" , "party_cd" , "party_desc" , "sex_code" , "sex" , "birth_age" , "birth_place" , "registr_dt" , "precinct_abbrv" , "precinct_desc" , "municipality_abbrv" , "municipality_desc" , "ward_abbrv" , "ward_desc" , "cong_dist_abbrv" , "cong_dist_desc" , "super_court_abbrv" , "super_court_desc" , "judic_dist_abbrv" , "judic_dist_desc" , "nc_senate_abbrv" , "nc_senate_desc" , "nc_house_abbrv" , "nc_house_desc" , "county_commiss_abbrv" , "county_commiss_desc" , "township_abbrv" , "township_desc" , "school_dist_abbrv" , "school_dist_desc" , "fire_dist_abbrv" , "fire_dist_desc" , "water_dist_abbrv" , "water_dist_desc" , "sewer_dist_abbrv" , "sewer_dist_desc" , "sanit_dist_abbrv" , "sanit_dist_desc" , "rescue_dist_abbrv" , "rescue_dist_desc" , "munic_dist_abbrv" , "munic_dist_desc" , "dist_1_abbrv" , "dist_1_desc" , "dist_2_abbrv" , "dist_2_desc" , "confidential_ind" , "age" , "ncid" , "vtd_abbrv" , "vtd_desc"])
precinctvotes_pandas = pandas.read_csv('precinctvotes.tsv', sep='\t', low_memory=False, names = ["county", "precinct", "total_votes", "romney_percentage"])


sqlite_db = sqlite3.connect('voters_sqlite.db')
sqlite_cursor = sqlite_db.cursor()

duck_db = duckdb.connect('voters_duck.db')
duck_cursor = duck_db.cursor()

query_01 = ''' SELECT COUNT(*) FROM ncvoters WHERE ncvoters.status_cd='A'  '''
query_02 = ''' SELECT  romney_percentage, county_id, precinct, sex_code, race_code, ethnic_code, birth_age
FROM precinct_votes
INNER JOIN ncvoters
ON ncvoters.precinct_desc=precinct_votes.precinct AND ncvoters.county_desc=precinct_votes.county
WHERE ncvoters.status_cd='A'  '''

def query_01_pandas():
	pandas_query = ncvoters_pandas[["county_id"]][(ncvoters_pandas.status_cd == 'A')]
	result = (pandas_query.county_id).count()
	print (result)
	return;

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

def query_03_pandas():
	return;

def query_03_sqlite():
	return;

def query_03_duckdb():
	return;



start = time.time()
query_02_pandas()
end = time.time()
print("Query in pandas : " +str(end - start))

start = time.time()
query_02_sqlite()
end = time.time()
print("Query in sqlite : " +str(end - start))

start = time.time()
query_02_duckdb()
end = time.time()
print("Query in duckdb : " +str(end - start))