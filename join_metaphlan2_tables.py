#!/usr/bin/env python3
"""Join MetaPhlAn2 tables"""
__author__ = "Fredrik Boulund"
__date__ = "2019"
__version__ = "0.1"

from sys import argv, exit
from collections import defaultdict
from pathlib import Path
import argparse
import logging

import pandas as pd


def parse_args():
    desc = f"{__doc__} v{__version__}. {__author__} (c) {__date__}."
    parser = argparse.ArgumentParser(description=desc, epilog="Version "+__version__)
    parser.add_argument("mpa_tables", nargs="+",
            help="MetaPhlAn2 TSV table(s) to join.")
    parser.add_argument("-o", "--outfile", dest="outfile",
            default="joined_metaphlan2_tables.tsv",
            help="Outfile name [%(default)s].")
    parser.add_argument("-f", "--force", action="store_true",
            default=False,
            help="Overwrite output file if it already exists [%(default)s].")
    parser.add_argument("-n", "--fillna", dest="fillna", metavar="FLOAT",
            default=0.0,
            type=float,
            help="Fill NA values in merged table with FLOAT [%(default)s].")
    parser.add_argument("-l", "--loglevel", choices=["INFO", "DEBUG"],
            default="INFO",
            help="Set logging level [%(default)s].")
    
    if len(argv) < 2:
        parser.print_help()
        exit()

    return parser.parse_args()


def get_sample_name_and_column_headers(mpa_table):
    """ Return a tuple with sample name, column headers, database, and table variant.

    Variant is an integer also indicating the number of rows to skip when parsing table.
    """
    with open(mpa_table) as f:
        first_line = f.readline().strip()
        second_line = f.readline().strip()
        third_line = f.readline().strip()
        fourth_line = f.readline().strip()

        if first_line.startswith("#SampleID"):
            columns = first_line.split()
            return columns[0], columns, "#Unknown database", 1
        if second_line.startswith("#SampleID"):
            db = first_line
            sample_name = second_line.split()[1]
            columns = third_line.split()
            return sample_name, columns, db, 2
        if third_line.startswith("#SampleID"):
            db = first_line
            sample_name = third_line.split()[1]
            columns = fourth_line.split()
            return sample_name, columns, db, 3

        raise NotImplementedError("No support for table type with first four lines like:\n %s\n %s\n %s\n %s" %
                (first_line, second_line, third_line, fourth_line))


def main(mpa_tables, outfile, fillna, overwrite):
    """Read all tables, join them, write to file."""
    tables = []
    observed_table_variants = defaultdict(list)
    observed_table_sizes = defaultdict(int)
    observed_databases = []
    for mpa_table in mpa_tables:
        sample_id, columns, db, variant = get_sample_name_and_column_headers(mpa_table)
        observed_table_variants[variant].append(mpa_table)
        observed_databases.append(db)
        tables.append(
            pd.read_table(mpa_table, skiprows=variant)\
            .set_index(columns[:-1])\
            .rename(columns={columns[-1]: sample_id})
        )
        observed_table_sizes[tables[-1].shape[0]] += 1 

    logging.debug("Loaded %s tables to join.", len(tables))
    logging.debug("Observed table sizes: %s.", dict(observed_table_sizes))
    if len(observed_table_variants) > 1:
        logger.warning("More than one table variant observed: %s.", dict(observed_table_variants))
    if len(set(observed_databases)) == 1:
        logger.debug("All tables used the same db: %s", observed_databases[0])
    else:
        logger.warning("Joined tables appears to have used different databases: %s", observed_databases)
    
    df = tables[0]
    for table in tables[1:]:
        df = df.join(table, how="outer")
    df.fillna(fillna, inplace=True)
    logger.debug("Output table has %s rows.", df.shape[0])

    if Path(outfile).exists() and not overwrite:
        logger.error("Output file '%s' already exists and --force is not set.", outfile)
        exit(2)
    df.to_csv(outfile, sep="\t")


if __name__ == "__main__":
    args = parse_args()
    logger = logging.getLogger(__name__)
    loglevels = {"INFO": logging.INFO, "DEBUG": logging.DEBUG}
    logging.basicConfig(format='%(levelname)s: %(message)s', level=loglevels[args.loglevel])

    if len(args.mpa_tables) < 2:
        print("Need at least two tables to merge!")
        exit(1)

    main(args.mpa_tables, args.outfile, args.fillna, args.force)
