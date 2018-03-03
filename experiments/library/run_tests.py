import shutil
import os
import subprocess
import time
import library_client
import sys
import pandas as pd

N_ACTIONS = 125
N_TRIALS = 50

def run_one_test(old_type, new_type, workload):
    shutil.copy("backup.db", "test.db")
    os.system('sqlite3 test.db "vacuum"')

    # Read in the file
    with open('library_server.rkt', 'r') as file :
      filedata = file.read()

    # Replace the target string
    filedata = filedata.replace("(define DBIMPL '%s)" % old_type, "(define DBIMPL '%s)" % new_type)

    # Write the file out again
    with open('library_server.rkt', 'w') as file:
      file.write(filedata)

    os.system("raco make library_server.rkt")

    server_p = subprocess.Popen("racket library_server.rkt", shell=True)
    # Give the server time to startup
    time.sleep(5)

    try:
        start = time.time()
        library_client.run_tests(N_ACTIONS, workload)
        end = time.time()
        return (end - start)
    finally:
        server_p.kill()


def main(workload, fsuffix):
    db_total = []
    sdb_total = []
    sdb_ctc_total = []

    start = time.time()

    for i in range(N_TRIALS):
        db_total.append(run_one_test("sdb-ctc", "db", workload))
        sdb_total.append(run_one_test("db", "sdb", workload))
        sdb_ctc_total.append(run_one_test("sdb", "sdb-ctc", workload))

    end = time.time()
    total_time = end - start

    print "Total time: " + str(total_time) + "\n"

    pd.DataFrame(db_total).to_csv("data/db_%s.csv" % fsuffix)
    pd.DataFrame(sdb_total).to_csv("data/shilldb_%s.csv" % fsuffix)
    pd.DataFrame(sdb_ctc_total).to_csv("data/shilldbctc%s.csv" % fsuffix)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])