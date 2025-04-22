import duckdb
import time
import csv

# load the queries from the SQL file
with open("tpch_queries_duckdb.sql", "r") as f:
    sql_text = f.read()

# split the queries by "-- Q" markers
queries = {}
for block in sql_text.split('-- Q'):
    if block.strip():
        qnum, query = block.split('\n', 1)
        queries[qnum.strip()] = query.strip()

def prepare_db(scale_factor):
    con = duckdb.connect(f"tmp/sf{scale_factor}.duckdb")  # save to a file
    con.execute(f"CALL dbgen(sf={scale_factor});")
    return con

def run_experiment(print_output:bool = False):
    query_ids = [i for i in range(1, 23)]
    times = {}

    for query_number in query_ids:
        try:
            start_time = time.time()
            result = con.execute(queries[str(query_number)])
            end_time = time.time()
            times[query_number] = end_time - start_time
            if print_output:
                print(f"Query {query_number} ran in {times[query_number]:.3f} seconds")
        except Exception as e:
            if print_output:
                print(f"Query {query_number} failed: {e}")
            times[query_number] = None 

    return times

def save_times_to_csv(times, scale_factor, filename='all_query_times.csv'):
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # if the file is empty, write the header
        if csvfile.tell() == 0:
            writer.writerow(['Scale Factor', 'Query Number', 'Time (seconds)'])
        
        # write times for this scale factor
        for query_num, runtime in times.items():
            writer.writerow([scale_factor, query_num, runtime])


for sf in [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
    print(f"Running experiment for scale factor {sf}...")
    con = prepare_db(sf)
    times = run_experiment(print_output=True)
    save_times_to_csv(times, sf)
