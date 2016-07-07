import os
import tarfile
import zipfile
from time import strftime, sleep
import re
import subprocess
from Bio import Entrez
import shutil
import boto3
import botocore


TEMP_FILES_PATH = os.getcwd()  # TODO: change path for server


def prepare_files(items, file_name, user_id):
    """Receives a list of items and a file to write them to, then writes them to file and returns the file path."""
    full_path = os.path.join(TEMP_FILES_PATH, "_".join([user_id, file_name]))
    with open(full_path, 'w') as f:
        for item in items:
            f.write(item + "\n")
    return full_path


# def move_file_to_s3(file_path):
#     """Move files to s3"""
#     # create file path in S3:
#     s3_path = ""
#
#     # copy file to S3
#
#     # delete local file
#     os.remove(file_path)
#
#     return s3_path


def zip_results(fasta_output_path, csv_file_path, output_path):
    """
    Receives a folder containing fasta sequences and a csv file, adds them all to zip.
    :param fasta_output_path:
    :param csv_file_path:
    :param output_path:
    :return:
    """
    zip_file = os.path.join(output_path, "output.zip")
    with zipfile.ZipFile(zip_file, mode='w') as zf:
        # adding all fasta files
        for fasta in os.listdir(fasta_output_path):
            zf.write(fasta)
        zf.write(csv_file_path)  # add csv file
    return zip_file


# debugging function
def debug_s(debug_string, to_debug):
    """
    Receives a string and prints it, with a timestamp.
    :param debug_string: a string to print
    :param to_debug: boolean flag: True means print, False - ignore.
    :return:
    """
    if to_debug:
        print "DEBUG {0}: {1}".format(strftime('%H:%M:%S'), debug_string)


def create_folder_if_needed(path):
    """
    Receives a path and creates a folder when needed (if it doesn't already exist).
    """
    if os.path.exists(path):
        print "{} dir exists".format(path)
    else:
        print "{} dir does not exist. Creating dir.".format(path)
        os.mkdir(path)


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


# def targz_list(archive_name, file_list):
#     """
#     Returns True after
#     :param archive_name:
#     :param file_list:
#     :return:
#     """
#     with tarfile.open(archive_name, "w:gz") as tar:
#         for file_name in file_list:
#             tar.add(file_name, arcname=os.path.basename(file_name))
#             os.remove(file_name)  # removing the sge file
#     return True


def targz_folder(archive_name, folder):
    """
    Returns True after
    :param archive_name:
    :param folder:
    :return:
    """
    with tarfile.open(archive_name, "w:gz") as tar:
        tar.add(folder, arcname=os.path.basename(folder))
    return True


def cleanup(path, storage_folder, run_id):  # TODO: problem with TAR, debug!
    """
    Performs tar and gzip on sets of files produced by the program.
    Then deletes the files and folders.
    :param path:  # the run_folder
    :param storage_folder:  # the folder containing fasta files
    :param run_id:  # the folder containing the first blast results
    :return:
    """
    # compress all files in path:
    # fasta_path
    path_archive = os.path.join(storage_folder, "{}.all.tar.gz".format(run_id))
    if targz_folder(path_archive, path):  # compress run_folder
        shutil.rmtree(path)  # delete run folder
    return True


def write_blast_run_script(command_line):
    """Writing a blast run script, and giving it run permissions."""
    script_path = "/tmp/blastp_run.sh"  # default script location
    with open(script_path, 'w') as script:
        script.write("#! /bin/tcsh\n")
        script.write("# The script is designed to run the following blastp command from RecBlast\n")
        script.write(command_line)
        # run permissions for the script:
    os.chmod(script_path, 0751)
    return script_path


def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z


def is_number(s):
    """
    The script determines if a string is a number or a text.
    Returns True if it's a number.
    """
    try:
        int(s)
        return True
    except ValueError:
        return False


# def blastdb_exit():
#     """Exiting if we can't find the $BLASTDB on the local machine"""
#     print("$BLASTDB was not found! Please set the blast DB path to the right location.")
#     print("Make sure blast+ is installed correctly.")
#     exit(1)


def exists_not_empty(path):
    """Receives a file path and checks if it exists and not empty."""
    if os.path.exists(path) and os.stat(path).st_size > 0:
        return True
    else:
        return False


def subset_db(tax_id, gi_file_path, db_path, big_db, run_anyway, DEBUG, debug, attempt_no=0):
    """
    Subsets a big blast database into a smaller one based on tax_id.
    The function connects to entrez and retrieves gi identifiers of sequences with the same tax_id.

    :param tax_id: The tax_id (string)
    :param gi_file_path: file path of the gi_list file we are creating
    :param db_path: the new db path
    :param big_db: we are about to subset
    :param run_anyway: run on NR if unable to subset
    :param attempt_no: counter for the attempts in connecting to Entrez (attempts to connect up to 10 times).
    :param DEBUG: A boolean flag: True for debug_func prints, False for quiet run.
    :param debug: A function call to provide debug_func prints.
    :return:
    """

    # connecting to ENTREZ protein DB
    try:
        handle = Entrez.esearch(db="protein", term="txid{}[ORGN]".format(tax_id), retmode="xml", retmax=10000000)
        record = Entrez.read(handle)

    except Exception, e:  # DB connection exception
        print "Error connecting to server, trying again..."
        print "Error: {}".format(e)
        debug("Error connecting to server, trying again...\n")

        # sleeping in case it's a temporary database problem
        sleep_period = 180
        print "restarting attempt in {} seconds...".format(sleep_period)
        sleep(sleep_period)

        # counting the number of attempts to connect.
        attempt_no += 1
        if attempt_no >= 10:  # If too many:
            print "Tried connecting to Entrez DB more than 10 times. Check your connection or try again later."
            raise e
            # exit(1)  # can't exit

        # try again (recursive until max)
        return subset_db(tax_id, gi_file_path, db_path, big_db, run_anyway, DEBUG, debug, attempt_no)

    assert int(record["Count"]) == len(record["IdList"]), "Did not fetch all sequences!"  # make sure we got it all...
    # writing a gi list file
    with open(gi_file_path, 'w') as gi_file:
        gi_file.write("\n".join(record["IdList"]) + "\n")

    # the new target database path
    create_folder_if_needed(os.path.join(db_path, tax_id))
    target_db = os.path.join(db_path, tax_id, "db")
    aliastool_command = ["blastdb_aliastool", "-gilist", gi_file_path, "-db", big_db, "-dbtype", "prot", "-out",
                         target_db]
    try:
        subprocess.check_call(aliastool_command)
        print("Created DB subset from nr protein for {}".format(tax_id))
        return target_db
    except subprocess.CalledProcessError:
        print("Problem with creating DB for tax_id {} from nr.".format(tax_id))
        if run_anyway:
            print("Running with the heavy nr option. Do some stretches, it might be a long run.")
            return big_db
        print("Aborting.\n"
              "If you want to run the program anyway against the entire nr "
              "(which is significantly slower than the default run, please use the --run_even_if_no_db_found flag.")
        raise e
        # exit(1)


def generate_download_link(user_id, expires=604800):
    """Generates S3 download link that expires after 1 week."""
    session = botocore.session.get_session()
    client = session.create_client('s3')
    presigned_url = client.generate_presigned_url('get_object', Params={'Bucket': 'recblastdata',
                                                                        'Key': '{}/output.zip'.format(user_id)},
                                                  ExpiresIn=expires)
    return presigned_url


# for efficiency
strip = str.strip
split = str.split
replace = str.replace
re_search = re.search
re_sub = re.sub
upper = str.upper
lower = str.lower
