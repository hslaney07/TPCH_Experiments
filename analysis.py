import duckdb
import time


# Load the SQL file
with open("tpch_queries_duckdb.sql", "r") as f:
    sql_text = f.read()

# Split the queries by "-- Q" markers
queries = {}
for block in sql_text.split('-- Q'):
    if block.strip():
        qnum, query = block.split('\n', 1)
        queries[qnum.strip()] = query.strip()


def run_experiment(print_output:bool = False):
    query_ids = [i for i in range(1, 23)]
    times = {}

    for query_number in query_ids:
        try:
            start_time = time.time()
            result = con.execute(queries[str(query_number)]).fetchdf()
            end_time = time.time()
            times[query_number] = end_time - start_time
            if print_output:
                print(f"Query {query_number} ran in {times[query_number]:.3f} seconds")
        except Exception as e:
            if print_output:
                print(f"Query {query_number} failed: {e}")
            times[query_number] = None  # or -1 to signify failure

    return times



import csv

def save_times_to_csv(times, scale_factor, filename='all_query_times.csv'):
    # Open the CSV file in append mode
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # If the file is empty, write the header
        if csvfile.tell() == 0:
            writer.writerow(['Scale Factor', 'Query Number', 'Time (seconds)'])
        
        # Write times for this scale factor
        for query_num, runtime in times.items():
            writer.writerow([scale_factor, query_num, runtime])


import duckdb

def prepare_db(scale_factor):
    con = duckdb.connect()  # This uses an in-memory database
    con.execute(f"CALL dbgen(sf={scale_factor});")
    return con


for sf in [1, 10, 100]:
    print(f"Running experiment for scale factor {sf}...")
    con = prepare_db(sf)
    times = run_experiment(print_output=False)
    save_times_to_csv(times, sf)
