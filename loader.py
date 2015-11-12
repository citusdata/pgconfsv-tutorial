import sys
import os
import gzip
import json
import psycopg2
import time
import datetime
import errno
import urllib
import multiprocessing
import signal

def redirect_load(arg):
    return redirect_load.func(arg)

def init_redirect_load(redirect_load, dsn):
    loader = Loader(dsn)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    redirect_load.func = loader.load_file

class Runner(object):

    def timeiter(self, a, b):
        delta = b - a

        start = datetime.datetime(a.year, a.month, a.day)

        for d in range(delta.days + 1):
            for h in range(0, 24):
                yield start + datetime.timedelta(days=d, seconds=h*3600)

    def __init__(self, dsn, startdate, enddate):
        self.dsn = dsn
        self.startdate = startdate
        self.enddate = enddate

    def run(self):
        pool = multiprocessing.Pool(None, init_redirect_load, [redirect_load, self.dsn])
        r = None
        try:
            results = pool.imap_unordered(redirect_load,
                self.timeiter(self.startdate, self.enddate))
            for result in results:
                print result
        except KeyboardInterrupt:
            pool.terminate()
            pool.join()

class Loader(object):
    prep = """
PREPARE insertdata AS
INSERT INTO data(github_id, type, public, created_at, actor, repo, org, payload)
VALUES($1, $2, $3, $4, $5, $6, $7, $8);
"""

    ins = """
EXECUTE insertdata(%(id)s, %(type)s, %(public)s, %(created_at)s,
       %(actor)s, %(repo)s, %(org)s, %(payload)s);
"""

    decoder = json.JSONDecoder()
    encoder = json.JSONEncoder()

    def __init__(self, dsn):
        self.dsn = dsn
        self.con = psycopg2.connect(dsn)
        self.cursor = self.con.cursor()
        self.cursor.execute(self.prep)

    def nicerow(self, json):
        row = {}
        for att in ('id', 'type', 'public', 'created_at'):
            row[att] = json[att]
        for jatt in ('actor', 'repo', 'org', 'payload'):
            if json.has_key(jatt):
                row[jatt] = self.encoder.encode(json[jatt])
            else:
                row[jatt] = None
        return row

    def parsefile(self, fname, transformer):
        with gzip.GzipFile(fname, 'r') as fh:
            for line in fh.readlines():
                if '\\u0000' in line:
                    continue
                json = self.decoder.decode(line)
                yield transformer(json)

    def retrieve_file(self, day):
        # gh data urls are weird - zero padded for month/day, truncated for hour
        fname = '%d-%.2d-%.2d-%d.json.gz' % (day.year, day.month, day.day, day.hour)
        fpath = os.path.join('data', fname)
        print fname

        try:
            os.stat(fpath)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
            urllib.urlretrieve('http://data.githubarchive.org/'+fname,
                               fpath+'.tmp')
            os.rename(fpath+'.tmp', fpath)
        return fpath

    def load_file(self, day):
        fpath = self.retrieve_file(day)

        start = time.time()
        rows = 0

        # process rows, throwing away the entire file on failures
        try:
            for row in self.parsefile(fpath, self.nicerow):
                rows += 1
                self.cursor.execute(self.ins, row)
            self.con.commit()
        except Exception, e:
            print "Error in file %s: %s" % (fpath, str(e))
            self.con.rollback()
        took = time.time() - start
        print "loaded %d rows in %f seconds, %f rows/sec" % (rows, took, rows/took)
        return (rows, took, rows/took)

runner = Runner(sys.argv[1], datetime.date(2015, 01, 01), datetime.date(2015,02,01))
runner.run()
