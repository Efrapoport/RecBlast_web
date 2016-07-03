#! /usr/bin/env python2

# from RecBlastParams import *
from RecBlastUtils import *
# import csv_transformer
# import taxa_to_taxid
# from uuid import uuid4
import part_one
import part_two
import part_three
from email_module import email_status

# value_list = [evalue, back_evalue, identity, coverage, string_similarity, gene_list, taxa_list,
#               reference_taxa, run_name, email, run_id]

# this will be the stand alone version of RecBlast for linux.


def debug_func(s):
    return debug_s(s, DEBUG)


def run_from_web(values_from_web, debug=debug_func):
    """

    :param values_from_web:
    :param debug:
    :return:
    """
    # (e_value_thresh, back_e_value_thresh, identity_threshold, coverage_threshold, textual_match, gene_list_file,
    #  taxa_list_file, reference_taxa, run_name, user_email, run_id, user_ip) = values_from_web  # old list
    (e_value_thresh, back_e_value_thresh, identity_threshold, coverage_threshold, textual_match, csv_path,
     taxa_list_file, origin_species, org_tax_id, run_name, user_email, run_id, user_ip) = values_from_web
    if back_e_value_thresh == "":
        back_e_value_thresh = e_value_thresh

    # BLAST PARAMS
    # defaults
    max_target_seqs = '1000000'
    max_attempts_to_complete_rec_blast = 100
    cpu = 1  # TODO: decide on CPU

    # fixed:
    outfmt = '6 staxids sseqid pident qcovs evalue sscinames sblastnames'
    accession_regex = re.compile(r'([A-Z0-9\._]+) ?')
    original_id = 1  # start part_two from 0. change this when you want to start from mid-file
    app_contact_email = "recblast@gmail.com"
    Entrez.email = app_contact_email
    textual_seq_match = 0.99  # comparison

    # DEBUG flags
    DEBUG = True  # TODO: change it

    # TODO: adapt
    # making sure the files we received exist and are not empty:  # TODO: move to web server???:

    # locating BLASTP path on your system  # TODO: set a fixed path!
    blastp_path = "Not valid"
    try:
        blastp_path = subprocess.check_output(["which", "blastp"], universal_newlines=True).strip()
        debug("BLASTP found in {}".format(blastp_path))
    except subprocess.CalledProcessError:
        print("No BLASTP found. Please check install blast properly or make sure it's in $PATH. Aborting.")
        exit(1)

    # script folder
    script_folder = os.path.dirname(os.path.abspath(__file__))

    # defining run folder
    # run_folder = os.getcwd()   # current folder  # TODO: decide on a basic folder depending on the host
    run_folder = os.path.join(storage_path, run_id)  # TODO STORAGE PATH??

    create_folder_if_needed(run_folder)  # creating the folder

    # creating the rest of the folders:
    # folders:
    first_blast_folder = os.path.join(run_folder, "first_blast")
    create_folder_if_needed(first_blast_folder)  # creating the folder
    second_blast_folder = os.path.join(run_folder, "second_blast")
    create_folder_if_needed(second_blast_folder)  # creating the folder
    fasta_path = os.path.join(run_folder, "fasta_path")
    create_folder_if_needed(fasta_path)
    fasta_output_folder = os.path.join(run_folder, "fasta_output")
    create_folder_if_needed(fasta_output_folder)
    csv_output_filename = os.path.join(run_folder, "output_table.csv")
    # Decide on taxa input:
    # tax_db = os.path.join(script_folder, "db/taxdump/tax_names.txt")  # moved to web server
    # database location
    db_folder = os.path.join(script_folder, "db")

    # moved to web server
    # # parsing and creating taxa files and parameters:
    # tax_name_dict = taxa_to_taxid.create_tax_dict(tax_db)
    # tax_id_dict = dict((v, k) for k, v in tax_name_dict.iteritems())  # the reverse dict
    # processing original species - moved to web server
    # working on the taxa list files - moved to web server

    # processing db information:
    #############################

    # forward db:
    db = "/blast/db/nr"  # default value. Doesn't matter at this point

    target_db_folder = os.path.join(db_folder, org_tax_id)  # this is where our db should be
    # check if it exists - if so use it as a db
    if os.path.exists(target_db_folder):
        target_db = os.path.join(target_db_folder, 'db')
        print "{} already has a local version of BLASTP db!" .format(org_tax_id)
    else:  # if not create an alias
        print("No local version of {} database exists. Creating a subset now.".format(org_tax_id))
        gi_file = os.path.join(run_folder, "taxon_gi_file_list.txt")  # the path to the new file
        target_db = subset_db(org_tax_id, gi_file, db_folder, db, False, DEBUG, debug)

    #################
    # Run main code #
    #################

    # TODO: send us emails about the user progress.
    if email_status(app_contact_email, run_name, run_id,
                    "Someone sent a new job on RecBlast online!\n"
                    "email: {0}, run name: {1}, , run_id: {2}, species of origin: {3} (taxid: {4}), ip: {5}\n"
                    "Started at: {6}".format(user_email, run_name, run_id, origin_species, org_tax_id, user_ip,
                                             strftime('%H:%M:%S'))):
        debug("email sent to {}".format(app_contact_email))
        # TODO: add details about the len of the taxa list and the gene_list_file
    if email_status(user_email, run_name, run_id,
                    "You have just sent a new job on RecBlast online!\n"
                    "Your job is now running on RecBlast online. The following are your run details:\n"
                    "Run name: {1}\nRun ID: {2}\nSpecies of origin: {3} (taxid: {4})\nStarted at: {5}".format(
                        user_email, run_name, run_id, origin_species, org_tax_id, user_ip, strftime('%H:%M:%S'))):
        debug("email sent to {}".format(app_contact_email))

    # print "Welcome to RecBlast."
    # print "Run {0} started at: {1}".format(run_name, strftime('%H:%M:%S'))

    # part 1:
    print("starting to perform part_one.py")
    id_dic, blast1_output_files = part_one.main(csv_path, app_contact_email, run_folder, fasta_path, first_blast_folder,
                                                fasta_output_folder, blastp_path, db, taxa_list_file, outfmt,
                                                max_target_seqs, e_value_thresh, coverage_threshold, cpu, DEBUG, debug)
    print("BLASTP part 1 done!")  # should find a way to make sure the data is okay...
    print("*******************")

    # part 2:
    second_blast_for_ids_dict, blast2_output_files, blast2_gene_id_paths = part_two.main(first_blast_folder,
                                                                                         second_blast_folder,
                                                                                         original_id, e_value_thresh,
                                                                                         identity_threshold,
                                                                                         coverage_threshold,
                                                                                         accession_regex, run_folder,
                                                                                         blastp_path, target_db, outfmt,
                                                                                         max_target_seqs,
                                                                                         back_e_value_thresh, cpu,
                                                                                         org_tax_id, DEBUG,
                                                                                         debug,
                                                                                         input_list=blast1_output_files)
    print("BLASTP part 2 done!")
    print("*******************")

    # part 3:
    if part_three.main(second_blast_folder, back_e_value_thresh, identity_threshold, coverage_threshold, textual_match,
                       textual_seq_match, origin_species, accession_regex, run_folder,
                       max_attempts_to_complete_rec_blast, csv_output_filename, fasta_output_folder, DEBUG, debug,
                       id_dic, second_blast_for_ids_dict, blast2_gene_id_paths):
        print("part 3 done!")
        print("*******************")

    # cleaning:
    if not DEBUG:
        if cleanup(run_folder, fasta_path, first_blast_folder, second_blast_folder):
            print("Files archived, compressed and cleaned.")

    print("Program done.")

    # Tell user about results  # TODO: edit and add link. and email us too!
    if email_status(user_email, run_name, run_id,
                    "Your job {} has just finished running on RecBlast online!\n"
                    "Your job is now running on RecBlast online. The following are your run details:\n"
                    "Run name: {1}\nRun ID: {2}\nSpecies of origin: {3} (taxid: {4})\nStarted at: {5}".format(
                        user_email, run_name, run_id, origin_species, org_tax_id, user_ip, strftime('%H:%M:%S'))):
        debug("email sent to {}".format(app_contact_email))


# TODO: change cleanup func
