#! /usr/bin/env python2

# from RecBlastParams import *
from RecBlastUtils import *
# import csv_transformer
# import taxa_to_taxid
# from uuid import uuid4
import part_one
import part_two
import part_three
from email_module import *
import users
import botocore
import boto3

# value_list = [evalue, back_evalue, identity, coverage, string_similarity, gene_list, taxa_list,
#               reference_taxa, run_name, email, run_id]

# this will be the stand alone version of RecBlast for linux.
DEBUG = True
global DEBUG


def debug_func(s):
    return debug_s(s, DEBUG)


def get_s3_client():
    # Hard coded strings as credentials, not recommended.
    AWS_ACCESS_KEY = 'AKIAISOVBXCFDI3V4XKA'
    AWS_SECRET_KEY = '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'
    os.environ['AWS_ACCESS_KEY'] = AWS_ACCESS_KEY
    os.environ['AWS_SECRET_KEY'] = AWS_SECRET_KEY
    print("Using access key: {0} and secret key: {1}".format(AWS_ACCESS_KEY, AWS_SECRET_KEY))
    return boto3.client('s3',
                        region_name='eu-central-1',
                        aws_access_key_id=AWS_ACCESS_KEY,
                        aws_secret_access_key=AWS_SECRET_KEY,
                        config=botocore.client.Config(signature_version='s3v4'))


def run_from_web(values_from_web, debug=debug_func):
    """

    :param values_from_web:
    :param debug:
    :return:
    """
    # (e_value_thresh, back_e_value_thresh, identity_threshold, coverage_threshold, textual_match, gene_list_file,
    #  taxa_list_file, reference_taxa, run_name, user_email, run_id, user_ip) = values_from_web  # old list
    (e_value_thresh, back_e_value_thresh, identity_threshold, coverage_threshold, textual_match, csv_file_content,
     taxa_file_content, origin_species, org_tax_id, run_name, user_email, run_id, user_ip) = values_from_web
    if back_e_value_thresh == "":
        back_e_value_thresh = e_value_thresh

    # BLAST PARAMS
    # defaults
    max_target_seqs = '1000000'
    max_attempts_to_complete_rec_blast = 100
    cpu = 8  # default cpu: 8

    # fixed:
    outfmt = '6 staxids sseqid pident qcovs evalue sscinames sblastnames'
    accession_regex = re.compile(r'([A-Z0-9\._]+) ?')
    description_regex = re.compile(r'\([^)]*\)')
    original_id = 1  # start part_two from 0. change this when you want to start from mid-file
    app_contact_email = "recblast@gmail.com"
    Entrez.email = app_contact_email
    textual_seq_match = 0.99  # comparison

    # DEBUG flags
    DEBUG = True  # TODO: change it
    global DEBUG

    # locating BLASTP path on your system
    blastp_path = "/usr/bin/blast/blastp"
    # try:
    #     blastp_path = subprocess.check_output(["which", "blastp"], universal_newlines=True).strip()
    #     debug("BLASTP found in {}".format(blastp_path))
    # except subprocess.CalledProcessError:
    #     print("No BLASTP found. Please check install blast properly or make sure it's in $PATH. Aborting.")
    #     exit(1)

    # script folder
    script_folder = os.path.dirname(os.path.abspath(__file__))
    storage_path = "/home/ubuntu/RecBlast/run_data/"
    s3_bucket_name = 'recblastdata'
    email_template = 'templates/email_templates/email_template.html'

    os.environ['BLASTDB'] = "/blast/db"  # setting the $blastdb # check if it workswq
    # defining run folder
    # run_folder = os.getcwd()   # current folder
    run_folder = os.path.join(storage_path, run_id)

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
    # s3_output_path = os.path.join(storage_path, 'output.zip')
    # Decide on taxa input:
    # tax_db = os.path.join(script_folder, "db/taxdump/tax_names.txt")  # moved to web server
    # database location
    db_folder = os.path.join(script_folder, "DB")

    # write csv file
    csv_path = os.path.join(run_folder, "input_genes.csv")
    with open(csv_path, 'w') as csv_file:
        csv_file.write(csv_file_content)
    # write taxa file
    taxa_list_file = os.path.join(run_folder, "taxa_file.csv")
    with open(taxa_list_file, 'w') as taxa_file:
        taxa_file.write(taxa_file_content)

    # moved to web server:
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
        print "{} already has a local version of BLASTP db!".format(org_tax_id)
    else:  # if not create an alias
        print("No local version of {} database exists. Creating a subset now.".format(org_tax_id))
        gi_file = os.path.join(run_folder, "taxon_gi_file_list.txt")  # the path to the new file
        target_db = subset_db(org_tax_id, gi_file, db_folder, db, False, DEBUG, debug)

    #################
    # Run main code #
    #################

    if email_status(app_contact_email, run_name, run_id,
                    "Someone sent a new job on RecBlast online!<br>"
                    "email: {0}, run name: {1}, run_id: {2}, species of origin: {3} (taxid: {4}), ip: {5}<br>"
                    "Started at: {6}".format(user_email, run_name, run_id, origin_species, org_tax_id, user_ip,
                                             strftime('%H:%M:%S')), email_template):
        debug("email sent to {}".format(app_contact_email))
    # TODO: add details about the len of the taxa list and the gene_list_file

    if email_status(user_email, run_name, run_id,
                    "You have just sent a new job on RecBlast online!<br>"
                    "Your job is now running on RecBlast online. The following are your run details:<br>"
                    "Run name: {0}<br>Run ID: {1}<br>Species of origin: {2} (taxid: {3})<br>Started at: {4}".format(
                        run_name, run_id, origin_species, org_tax_id, strftime('%H:%M:%S')),
                    email_template):
        debug("email sent to {}".format(user_email))

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
                       textual_seq_match, origin_species, accession_regex, description_regex, run_folder,
                       max_attempts_to_complete_rec_blast, csv_output_filename, fasta_output_folder, DEBUG, debug,
                       id_dic, second_blast_for_ids_dict, blast2_gene_id_paths):
        print("part 3 done!")
        print("*******************")

    # Zip results:
    zip_output_path = zip_results(fasta_output_folder, csv_output_filename, run_folder)
    print("saved zip output to: {}".format(zip_output_path))

    # S3 client
    s3 = get_s3_client()
    s3.upload_file(zip_output_path, s3_bucket_name, '{}/output.zip'.format(run_id))
    download_url = generate_download_link(run_id)
    print("Generated the following link:\n{}".format(download_url))
    # set the download url for the user:
    users.set_result_for_user_id(run_id, download_url)

    # # cleaning:
    if not DEBUG:  # compresses and cleans everything
        if cleanup(run_folder, storage_path, run_id):
            print("Files archived, compressed and cleaned.")
            # TODO: move to s3 for temporary storage

    result_page = "http://www.reciprocalblast.com/results/{}".format(run_id)
    print("Program done.")

    # Tell user about results
    if email_status(user_email, run_name, run_id,
                    "Your job {0} has just finished running on RecBlast online!<br>"
                    "The results can be found and downloaded from here:<br>{1}<br>"
                    "Run name: {1}<br>Run ID: {2}<br>Species of origin: {3} (taxid: {4})<br>Finished at: {5}<br><br>"
                    "Thanks for using RecBlast!".format(
                        run_name, result_page, run_id, origin_species, org_tax_id, strftime('%H:%M:%S')),
                    email_template):
        debug("email sent to {}".format(user_email))

    if email_status(user_email, run_name, run_id,
                    "User {0} job {1} has just finished running on RecBlast online!<br>"
                    "The results can be found and downloaded from here:<br>{2}<br>"
                    "Run name: {1}<br>Run ID: {3}<br>Species of origin: {4} (taxid: {5})<br>Finished at: {6}<br><br>"
                    "Thanks for using RecBlast!".format(
                        user_email, run_name, result_page, run_id, origin_species, org_tax_id, strftime('%H:%M:%S')),
                    email_template):
        debug("email sent to {}".format(app_contact_email))

    # user does not have a running job anymore:
    users.delete_email(user_email)  # deletes user email
    users.delete_user_id_for_email(user_email)
    return True


# create new s3 folder for user
# move result zip to s3
# set expiration date for s3 file
# del user from redis
# send email to user with the result path (and to us!)
