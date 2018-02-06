import shutil
import os
import subprocess
import time
import library_client

N_ACTIONS = 125
N_TRIALS = 50

def run_one_test(old_type, new_type):
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
        library_client.run_tests(N_ACTIONS)
        end = time.time()
        return (end - start)
    finally:
        server_p.kill()


def main():
    db_total = 0
    sdb_total = 0
    sdb_ctc_total = 0

    for i in range(N_TRIALS):
        db_total += run_one_test("sdb-ctc", "db")
        sdb_total += run_one_test("db", "sdb")
        sdb_ctc_total += run_one_test("sdb", "sdb-ctc")

    print "\n\n"

    print db_total
    print sdb_total
    print sdb_ctc_total

    print "\n\n"

    print db_total / N_TRIALS
    print sdb_total / N_TRIALS
    print sdb_ctc_total / N_TRIALS

if __name__ == '__main__':
    main()