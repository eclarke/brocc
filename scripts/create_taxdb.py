import hashlib
import subprocess
import urllib2
import os
import gzip
import mmap
import contextlib
import sqlite3
import csv
import itertools
from tqdm import tqdm

schema = """\
DROP TABLE IF EXISTS accn_taxid;
CREATE TABLE accn_taxid(
   accn_ver CHARACTER NOT NULL PRIMARY KEY,
   taxid INT NOT NULL);
"""

import_command = """\
.separator \\t
.import {} accn_taxid
"""


def _prep_database(db_fp):
    conn = sqlite3.connect(db_fp)
    conn.executescript(schema)
    conn.commit()
    conn.close()


def _insert_many(accn2taxid, db_fp, chunk_size=1000000):
    con = sqlite3.connect(db_fp)

    con.text_factory = str
    cur = con.cursor()
    # f = open(accn2taxid, 'rb')
    # mapped = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

    try:
        with gzip.open(accn2taxid) as infile:
            infile.next()
            while True:
                chunk = list(itertools.islice(infile, chunk_size))
                if not chunk:
                    break
                pbar = tqdm(csv.reader(chunk, delimiter='\t'),
                            unit='lines', total=len(chunk))
                for _, accn_var, taxid, _ in pbar:
                    cur.execute(
                        "INSERT OR IGNORE INTO accn_taxid VALUES (?,?)",
                        (accn_var, taxid))
            con.commit()
    finally:
        con.close()


def _check_md5(md5_url, filename):
    req = urllib2.Request(md5_url)
    print("downloading hash")
    remote_md5 = urllib2.urlopen(req).read().split(' ')[0]
    print("hashing file")
    this_md5 = hashlib.md5(open(filename, 'rb').read()).hexdigest()
    print(remote_md5, this_md5)
    return remote_md5 == this_md5


def download_nucl_gb_taxid(outfile):
    base_url = "ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid"
    url = "/".join([base_url, "nucl_gb.accession2taxid.gz"])
    md5_url = url + ".md5"
    if os.path.exists(outfile) and _check_md5(md5_url, outfile):
        print("File already exists and has the same MD5 hash; skipping.")
        return

    wget_available = subprocess.call(['command', '-v', 'wget']) == 0
    curl_available = subprocess.call(['command', '-v', 'curl']) == 0
    if wget_available:
        subprocess.check_call(
            ['wget', url, '-O', outfile])
    elif curl_available:
        subprocess.check_call(
            ['curl', '-o', outfile, url])
    else:
        raise RuntimeError(
            "Neither wget nor curl was found: cannot download taxonomy ids.")


def build_taxdict(accn2taxid_gz_fp, db_fp):
    with gzip.open(accn2taxid_gz_fp) as accn2taxid:
        # Skip header
        accn2taxid.next()
        with contextlib.closing(dbm.open(db_fp, 'c')) as db:
            for line in tqdm(accn2taxid):
                accn, accn_ver, taxid, gi = line.strip().split()
                db[accn_ver] = taxid


def load_taxdict(db_fp):
    return dbm.open(db_fp)
