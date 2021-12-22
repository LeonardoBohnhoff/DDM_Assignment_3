from cassandra.cluster import Cluster
import pandas as pd

# define helper function
def handle_potential_null_number(number):
    if pd.isnull(number):
        return 0
    else:
        return int(number)

# connect to local cluster and load keyspace
cluster = Cluster()
session = cluster.connect('covid')

# load records from Google Cloud Platform
gender_evolutions = pd.read_csv('by-sex.csv', sep=r',', skipinitialspace=True)

# insert relevant records into Cassandra table

insertion_query = "INSERT INTO covid.gender_evolution_by_country_province_municipality_date (country, province, municipality, day, new_confirmed_male, new_confirmed_female, new_hospitalized_patients_male, new_hospitalized_patients_female) VALUES(?,?,?,?,?,?,?,?)"
prepared_insertion_query = session.prepare(insertion_query)

# only insert the records from Germany which include a municipality
for index, row in gender_evolutions.iterrows():
    if row['location_key'].split('_')[0] == 'DE' and len(row['location_key'].split('_')) == 3:
        session.execute(prepared_insertion_query, (
            row['location_key'].split('_')[0],
            row['location_key'].split('_')[1],
            row['location_key'].split('_')[2],
            row['date'],
            handle_potential_null_number(row['new_confirmed_male']),
            handle_potential_null_number(row['new_confirmed_female']),
            handle_potential_null_number(row['new_hospitalized_patients_male']),
            handle_potential_null_number(row['new_hospitalized_patients_female']),
        ))

# retrieve records from table
selection_query = "SELECT day, new_confirmed_male, new_confirmed_female, new_hospitalized_patients_male, new_hospitalized_patients_female FROM covid.gender_evolution_by_country_province_municipality_date WHERE country=? AND province=? AND municipality=? AND day>? ORDER BY day ASC"
prepared_selection_query = session.prepare(selection_query)
country = 'DE'
province = 'BW'
municipality = '08118'
day = '2020-02-24'
results = session.execute(prepared_selection_query, (country, province, municipality, day))

# print records to console
for result in results:
    print('Date: {0}, Number of newly infected men: {1}, Number of newly infected women: {2}, Number of newly hospitalized men: {3}, Number of newly hospitalized women: {4}'.format(result.day, result.new_confirmed_male, result.new_confirmed_female, result.new_hospitalized_patients_male, result.new_hospitalized_patients_female))