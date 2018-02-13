import sys
import os
import time
import shutil
from pprint import pprint
import pandas as pd

NTRIALS = 100
MIN_SELECT = 0
MAX_SELECT = 100
INCR = 10

def run_one_test(qtype, selectivity):
    def db_setup():
        shutil.copy("backup.db", "test.db")
        os.system('sqlite3 test.db "vacuum"')
    def open_db():
        return os.popen("racket client.rkt 0 %d %s" % (selectivity, qtype))
    def open_shilldb():
        return os.popen("racket client.rkt 1 %d %s" % (selectivity, qtype))

    db_setup()

    db_start = time.time()
    p = open_db()
    _ = p.read()
    _ = p.close()
    db_end = time.time()
    db_total = db_end - db_start

    db_setup()

    shilldb_start = time.time()
    p = open_shilldb()
    _ = p.read()
    _ = p.close()
    shilldb_end = time.time()
    shilldb_total = shilldb_end - shilldb_start

    return db_total, shilldb_total

def main(arg):
    db_total = None
    sdb_total = None

    start = time.time()

    # Compile to bytecode
    os.system("raco make client.rkt")

    if arg == "insert":
        db_total = []
        sdb_total = []
    else:
        db_total = {i: [] for i in range(MIN_SELECT, MAX_SELECT + INCR, INCR)}
        sdb_total = {i: [] for i in range(MIN_SELECT, MAX_SELECT + INCR, INCR)}

    for i in range(NTRIALS):
        if arg == "insert":
            db, sdb = run_one_test(arg, 0)
            db_total.append(db)
            db_total.append(db)
        else:
            for s in range(MIN_SELECT, MAX_SELECT + INCR, INCR):
                db, sdb = run_one_test(arg, s)

                db_total[s].append(db)
                sdb_total[s].append(sdb)

    end = time.time()
    total_time = end - start

    print "Trials: " + str(NTRIALS) + "\n"
    print "Total time: " + str(total_time) + "\n"

    pd.DataFrame.from_dict(db_total).to_csv("db.csv")
    pd.DataFrame.from_dict(sdb_total).to_csv("shilldb.csv")

if __name__ == '__main__':
    main(sys.argv[1])
