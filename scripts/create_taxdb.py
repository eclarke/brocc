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
import argparse
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
    return conn


def _insert_many(accn2taxid, db_fp=None, chunk_size=1000000, conn=None):
    if db_fp is None and conn is None:
        raise ValueError("Must specify either a db file or a connection")
    
    con = sqlite3.connect(db_fp) if not conn else conn

    con.text_factory = str
    cur = con.cursor()
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

def _insert_many_streaming(accn2taxid, db_fp, conn = None):
    con = sqlite3.connect(db_fp) if not conn else conn
    con.text_factory = str
    cur = con.cursor()
    infile = gzip.open(accn2taxid)
    try:
        reader = tqdm(csv.reader(infile, delimiter='\t'), unit='lines', total=111000000)
        for _, accn_var, taxid, _ in reader:
            cur.execute(
                "INSERT OR IGNORE INTO accn_taxid VALUES (?,?)", (accn_var, taxid))
        con.commit()
    finally:
        con.close()
        infile.close()
        
        

def _check_md5(md5_url, filename):
    req = urllib2.Request(md5_url)
    remote_md5 = urllib2.urlopen(req).read().split(' ')[0]
    this_md5 = hashlib.md5(open(filename, 'rb').read()).hexdigest()
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
    
def main():

    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--db_fp',
        default="taxids.db",
        help="Name of database to create")

    parser.add_argument(
        '--force', '-f',
        action="store_true",
        help="Overwrite database if it exists")

    parser.add_argument(
        '--accn2taxid_fp',
        help="Path to accession2taxid gzipped file. If unspecified, will be downloaded.")

    args = parser.parse_args()

    if os.path.exists(args.db_fp) and not args.force:
        print("Database file already exists. Specify --force to overwrite.")
        return
    else:
        
    

    
    
