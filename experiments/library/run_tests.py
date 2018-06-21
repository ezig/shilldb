import shutil
import os
import subprocess
import time
import library_client
import sys
import re
import pandas as pd

N_ACTIONS = 125
N_TRIALS = 1

def run_one_test(new_type, workload):
    shutil.copy("backup.db", "test.db")
    os.system('sqlite3 test.db "vacuum"')

    # Read in the file
    lines = []
    with open('library_server.rkt', 'r') as file:
        for line in file:
            lines.append(re.sub(r"\(define DBIMPL .*\)", "(define DBIMPL '%s)" % new_type, line))

    # Replace the target string
    filedata = "".join(lines)

    # Write the file out again
    with open('library_server.rkt', 'w') as file:
      file.write(filedata)

    os.system("raco make library_server.rkt")

    server_p = subprocess.Popen("raco profile library_server.rkt >> output.txt", shell=True)
    # Give the server time to startup
    time.sleep(5)

    try:
        start = time.time()
        library_client.run_tests(N_ACTIONS, workload)
        end = time.time()
        return (end - start)
    finally:
        server_p.kill()

def set_triggers(enable):
    new_value = "#t" if enable else "#f"

    lines = []
    with open('../../private/api/db_conn.rkt', 'r') as file:
        for line in file:
            lines.append(re.sub(r"\(define install\? .*\)", "(define install? %s)" % new_value, line))

    # Replace the target string
    filedata = "".join(lines)

    # Write the file out again
    with open('../../private/api/db_conn.rkt', 'w') as file:
      file.write(filedata)

def main(workload, fsuffix):
    db_total = []
    sdb_no_trigger_total = []
    sdb_total = []
    sdb_ctc_total = []

    start = time.time()

    for i in range(N_TRIALS):
        # db_total.append(run_one_test("db", workload))
        # set_triggers(False)
        # sdb_no_trigger_total.append(run_one_test("sdb", workload))
        # set_triggers(True)
        # sdb_total.append(run_one_test("sdb", workload))
        sdb_ctc_total.append(run_one_test("sdb-ctc", workload))

    end = time.time()
    total_time = end - start

    print("Total time: " + str(total_time) + "\n")

    # pd.DataFrame(db_total).to_csv("data/db_%s.csv" % fsuffix)
    # pd.DataFrame(sdb_no_trigger_total).to_csv("data/shilldb_no_trigger_%s.csv" % fsuffix)
    # pd.DataFrame(sdb_total).to_csv("data/shilldb_%s.csv" % fsuffix)
    # pd.DataFrame(sdb_ctc_total).to_csv("data/shilldbctc_%s.csv" % fsuffix)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])