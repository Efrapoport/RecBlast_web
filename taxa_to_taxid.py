#! /usr/bin/env python2
from RecBlastUtils import is_number, strip
from taxa import *


def create_tax_dict(tax_db):
    """
    Receives a tax_db (which is an database parsed from tax_dump from ncbi.
    :param tax_db: A tab delimited file formatted as: <tax id>\t<tax name>
    :return: tax_dict
    """
    with open(tax_db, 'r') as db:  # reading the tax_db file
        tax_list = [x.strip().split('\t') for x in db.readlines()]  # parsing to a list
        # parsing to a dict where key=tax_name, value=tax_id
        tax_dict = dict([(x[1], x[0]) for x in tax_list])
    return tax_dict


def convert_tax_to_taxid(tax_list_file, output_path=None):
    """
    Receives a dictionary of taxa and their tax_id, a file containing taxa name, and an optional output path.
    :param tax_list_file: a list of the tax names we want to keep
    :param output_path: (optional) output for the filtered tax_id list
    :return:
    """
    bad_tax_list = []
    good_tax_list = []
    line_counter = 0
    if not output_path:  # if the output path is not provided, let's create our own
        output_path = tax_list_file + ".taxid.txt"
    with open(tax_list_file) as f:
        with open(output_path, "w") as output:  # writing output file
            for line in f:
                line = strip(line)
                if not line:
                    continue
                try:
                    tax_id = get_value_by_name(line)
                    if line.find(" ") == -1:
                        print "Taxon {} is not a valid taxon. Too big of a clade!".format(line)
                        bad_tax_list.append(line)
                    else:
                        output.write("{}\n".format(tax_id))
                        good_tax_list.append(line)  # append name
                except KeyError:
                    if is_number(line):  # guess it's already a tax id
                        try:
                            tax_name = get_name_by_value(line)
                            tax_id = line
                            output.write("{}\n".format(tax_id))
                            good_tax_list.append(tax_name)  # append name
                        except KeyError:  # If it's not a valid tax ID
                            print "Tax ID {} does not exist in NCBI tax database!".format(line)
                            bad_tax_list.append(line)
                    else:
                        print "Taxon {} does not exist in NCBI tax database!".format(line)
                        bad_tax_list.append(line)
                line_counter += 1
    if len(bad_tax_list) == line_counter:
        print("No valid tax name or tax_id provided! exiting.")
        return output_path, bad_tax_list, good_tax_list, False
    # returning the list of the bad taxa
    return output_path, bad_tax_list, good_tax_list, True
