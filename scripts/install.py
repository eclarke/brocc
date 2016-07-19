import hashlib
import subprocess
import urllib2
import os
import gzip
import argparse

from brocclib.database import TaxIdDb


def _check_md5(md5_url, filename):
    print("Detected existing file, checking integrity...")
    req = urllib2.Request(md5_url)
    remote_md5 = urllib2.urlopen(req).read().split(' ')[0]
    print("  Remote file hash:\t{}".format(remote_md5))
    this_md5 = hashlib.md5(open(filename, 'rb').read()).hexdigest()
    print("  Local file hash:\t{}".format(this_md5))
    return remote_md5 == this_md5


def download_nucl_gb_taxid(outfile):
    base_url = "ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid"
    url = "/".join([base_url, "nucl_gb.accession2taxid.gz"])
    md5_url = url + ".md5"
    if os.path.exists(outfile) and _check_md5(md5_url, outfile):
        print("File already exists and has the same MD5 hash; skipping.")
        return

    # Try using wget
    if subprocess.call(['command', '-v', 'wget'], shell=True) == 0:
        subprocess.check_call(
            ['wget', url, '-O', outfile])
    # Try using curl
    elif subprocess.call(['command', '-v', 'curl'], shell=True) == 0:
        subprocess.check_call(
            ['curl', '-o', outfile, url])
    else:
        raise RuntimeError(
            "Neither wget nor curl was found: cannot download taxonomy ids.")


def main():

    parser = argparse.ArgumentParser(
        description="Download the accession2taxid file and prepare it for use with BROCC.")

    parser.add_argument(
        '--db_fp',
        metavar="DB_PATH",
        default="taxids.db",
        help="Name of database to create")

    parser.add_argument(
        '--force', '-f',
        action="store_true",
        help="Overwrite database if it exists")

    parser.add_argument(
        '--accn2taxid_fp',
        metavar="PATH",
        default="nucl_gb.accession2taxid.gz",
        help=(
            "Path to accession2taxid gzipped file. If unspecified, will be "
            "downloaded."))

    parser.add_argument(
        '--chunk_size',
        metavar="INT",
        default=10000000,
        help=(
            "Default number of records to insert at a time. Lower this if "
            "you have memory issues. Default: %(default)s"))

    args = parser.parse_args()

    if os.path.exists(args.db_fp) and not args.force:
        print("Database file already exists. Specify --force to overwrite.")
        exit(1)

    # --- Download the accn2taxid file from NCBI

    download_nucl_gb_taxid(args.accn2taxid_fp)

    # --- Create the database
    print("Creating database...")
    with TaxIdDb(args.db_fp, create=True) as db:
        with gzip.open(args.accn2taxid_fp) as infile:
            db.populate(infile)

    print("Finished. Database created at {}".format(args.db_fp))

if __name__ == "__main__":
    main()
