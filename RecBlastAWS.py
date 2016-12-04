#! /usr/bin/env python2

from RecBlastUtils import *
from RecBlastFigures import *
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

# this is the RecBlast web server version.
__version__ = "1.1.1"

# value_list = [evalue, back_evalue, identity, coverage, string_similarity, gene_list, taxa_list,
#               reference_taxa, run_name, email, run_id]

# this will be the stand alone version of RecBlast for linux.
DEBUG = True
global DEBUG


def debug_func(s):
    """Wrapper for the debug print function"""
    return debug_s(s, DEBUG)


def run_from_web(values_from_web, debug=debug_func):
    """Running the entire RecBlast program from the web server interface. """
    # (e_value_thresh, back_e_value_thresh, identity_threshold, coverage_threshold, textual_match, gene_list_file,
    #  taxa_list_file, reference_taxa, run_name, user_email, run_id, user_ip) = values_from_web  # old list
    # receiving values from the web server:
    (e_value_thresh, back_e_value_thresh, identity_threshold, coverage_threshold, textual_match, csv_file_content,
     taxa_file_content, origin_species, org_tax_id, run_name, user_email, run_id, user_ip,
     tax_list_original_input, gene_list_original_input, good_tax_list) = values_from_web

    if back_e_value_thresh == "":
        back_e_value_thresh = e_value_thresh

    # BLAST PARAMS
    # defaults
    max_target_seqs = '1000000'
    max_attempts_to_complete_rec_blast = 100
    cpu = 8  # default cpu: 8
    run_all = False

    # fixed:
    outfmt = '6 staxids sseqid pident qcovs evalue sscinames sblastnames'
    accession_regex = re.compile(r'([A-Z0-9\._]+) ?')
    description_regex = re.compile(r'\([^)]*\)')
    original_id = 1  # start part_two from 0. change this when you want to start from mid-file
    app_contact_email = "recblast@gmail.com"
    Entrez.email = app_contact_email
    textual_seq_match = 0.99  # comparison

    # DEBUG flags
    DEBUG = True
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
    run_folder = join_folder(storage_path, run_id)

    create_folder_if_needed(run_folder)  # creating the folder

    # creating the rest of the folders:
    # folders:
    first_blast_folder = join_folder(run_folder, "first_blast")
    create_folder_if_needed(first_blast_folder)  # creating the folder
    second_blast_folder = join_folder(run_folder, "second_blast")
    create_folder_if_needed(second_blast_folder)  # creating the folder
    fasta_path = join_folder(run_folder, "fasta_path")
    create_folder_if_needed(fasta_path)
    fasta_output_folder = join_folder(run_folder, "fasta_output")
    create_folder_if_needed(fasta_output_folder)
    csv_rbh_output_filename = join_folder(run_folder, "output_table_RBH.csv")
    csv_strict_output_filename = join_folder(run_folder, "output_table_strict.csv")
    csv_ns_output_filename = join_folder(run_folder, "output_table_non-strict.csv")
    # s3_output_path = join_folder(storage_path, 'output.zip')
    # Decide on taxa input:
    # tax_db = join_folder(script_folder, "db/taxdump/tax_names.txt")  # moved to web server
    # database location
    db_folder = join_folder(script_folder, "DB")

    # write csv file
    csv_path = join_folder(run_folder, "input_genes.csv")
    with open(csv_path, 'w') as csv_file:
        csv_file.write(csv_file_content)
    # write taxa file
    taxa_list_file = join_folder(run_folder, "taxa_file.csv")
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

    target_db_folder = join_folder(db_folder, org_tax_id)  # this is where our db should be
    # check if it exists - if so use it as a db
    if os.path.exists(target_db_folder):
        target_db = join_folder(target_db_folder, 'db')
        print "{} already has a local version of BLASTP db!".format(org_tax_id)
    else:  # if not create an alias
        print("No local version of {} database exists. Creating a subset now.".format(org_tax_id))
        gi_file = join_folder(run_folder, "taxon_gi_file_list.txt")  # the path to the new file
        target_db = subset_db(org_tax_id, gi_file, db_folder, db, False, DEBUG, debug)

    #################
    # Run main code #
    #################

    if email_status(app_contact_email, run_name, run_id,
                    "Someone sent a new job on RecBlast online!<br>"
                    "email: {0}, run name: {1}, run_id: {2}, species of origin: {3} (taxid: {4}), ip: {5}<br>"
                    "Started at: {6}<br>"
                    "Number of input taxa (before validation!): {7}<br>"
                    "Number of input genes (before validation!): {8}<br>"
                    "With the following taxa: {9}<br>".format(user_email, run_name, run_id, origin_species, org_tax_id,
                                                              user_ip, strftime('%H:%M:%S'),
                                                              len(tax_list_original_input),
                                                              len(gene_list_original_input),
                                                              ",".join(tax_list_original_input)), email_template):
        debug("email sent to {}".format(app_contact_email))

    if email_status(user_email, run_name, run_id,
                    "Your job is now running on RecBlast online. The following are your run details:<br>"
                    "Run name: {0}<br>Run ID: {1}<br>Species of origin: {2} (taxid: {3})<br>"
                    "Started at: {4}".format(
                        run_name, run_id, origin_species, org_tax_id, strftime('%H:%M:%S')), email_template):
        debug("email sent to {}".format(user_email))

    # part 1:
    print("starting to perform part_one.py")
    id_dic, blast1_output_files, local_id_dic = part_one.main(csv_path, app_contact_email, run_folder, fasta_path,
                                                              first_blast_folder, fasta_output_folder, blastp_path, db,
                                                              taxa_list_file, outfmt, max_target_seqs, e_value_thresh,
                                                              coverage_threshold, cpu, run_all, DEBUG, debug)
    print("BLASTP part 1 done!")  # should find a way to make sure the data is okay...
    print("*******************")

    # part 2:
    second_blast_for_ids_dict, blast2_output_files, blast2_gene_id_paths = part_two.main(local_id_dic,
                                                                                         first_blast_folder,
                                                                                         second_blast_folder,
                                                                                         original_id, e_value_thresh,
                                                                                         identity_threshold,
                                                                                         coverage_threshold,
                                                                                         accession_regex, run_folder,
                                                                                         blastp_path, target_db, outfmt,
                                                                                         max_target_seqs,
                                                                                         back_e_value_thresh, cpu,
                                                                                         org_tax_id, run_all, DEBUG,
                                                                                         debug,
                                                                                         input_list=blast1_output_files)
    print("BLASTP part 2 done!")
    print("*******************")

    # part 3:
    if part_three.main(second_blast_folder, back_e_value_thresh, identity_threshold, coverage_threshold, textual_match,
                       textual_seq_match, origin_species, accession_regex, description_regex, run_folder,
                       max_attempts_to_complete_rec_blast, csv_rbh_output_filename, csv_strict_output_filename,
                       csv_ns_output_filename, fasta_output_folder, DEBUG, debug,
                       good_tax_list, id_dic, second_blast_for_ids_dict, blast2_gene_id_paths):
        print("part 3 done!")
        print("*******************")

    # Visual output:
    debug("Creating images:")
    image_paths = generate_visual_graphs(csv_rbh_output_filename, csv_strict_output_filename,
                                         csv_ns_output_filename)
    debug("Image paths saved!")

    # Zip results:
    files_to_zip = [csv_rbh_output_filename, csv_strict_output_filename, csv_ns_output_filename] + image_paths.values()
    # files_to_zip = [csv_rbh_output_filename, csv_strict_output_filename, csv_ns_output_filename] + \
    #                [join_folder(run_folder, '{}.png'.format(x)) for x in image_paths.keys()]
    # zip_output_path = zip_results(fasta_output_folder, csv_rbh_output_filename, csv_strict_output_filename,
    #                               csv_ns_output_filename, run_folder)
    zip_output_path = zip_results(fasta_output_folder, files_to_zip, run_folder)
    print("saved zip output to: {}".format(zip_output_path))

    # S3 client
    s3 = get_s3_client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    debug("Connected to s3 client")

    # uploading ZIP file
    s3.upload_file(zip_output_path, s3_bucket_name, '{}/output.zip'.format(run_id))
    debug("Uploaded file.")
    download_url = generate_download_link(run_id, 'output.zip', AWS_ACCESS_KEY, AWS_SECRET_KEY)
    print("Generated the following link:\n{}".format(download_url))
    # set the download url for the user:
    users.set_result_for_user_id(run_id, download_url)

    # uploading graphs
    for image in image_paths:
        image_name = os.path.basename(image_paths[image])
        image_path = '{}/{}'.format(run_id, image_name)
        s3.upload_file(image_paths[image], s3_bucket_name, image_path)  # uploading the file
        debug("Uploaded image {}".format(image_name))
        download_url = generate_download_link(run_id, image_name, AWS_ACCESS_KEY, AWS_SECRET_KEY)
        users.set_image_for_user_id(run_id, image, download_url)
        debug("uploaded image {} to {}".format(image, download_url))
    debug("Uploaded images.")

    # # cleaning:
    if not DEBUG:  # compresses and cleans everything
        if cleanup(run_folder, storage_path, run_id):
            print("Files archived, compressed and cleaned.")
            # TODO: move to s3 for temporary storage

    result_page = "http://reciprocalblast.com/results/{}".format(run_id)
    print("Program done.")

    # Tell user about results
    if email_status(user_email, run_name, run_id,
                    "Your job {0} has just finished running on RecBlast online!<br>"
                    "The results can be found and downloaded from here:<br>{1}<br>"
                    "Run name: {0}<br>Run ID: {2}<br>Species of origin: {3} (taxid: {4})<br>Finished at: {5}<br><br>"
                    "Thanks for using RecBlast!".format(
                        run_name, result_page, run_id, origin_species, org_tax_id, strftime('%H:%M:%S')),
                    email_template):
        debug("email sent to {}".format(user_email))

    if email_status(app_contact_email, run_name, run_id,
                    "User {0} job '{1}' has just finished running on RecBlast online!<br>"
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

