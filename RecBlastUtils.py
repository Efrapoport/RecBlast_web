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
import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns


TEMP_FILES_PATH = os.getcwd()  # TODO: change path for server


def prepare_files(items, file_name, user_id, files_path=TEMP_FILES_PATH):
    """Receives a list of items and a file to write them to, then writes them to file and returns the file path."""
    full_path = join_folder(files_path, "_".join([user_id, file_name]))
    # items = list(set(items))  # make the list unique  # unnecessary
    with open(full_path, 'w') as f:
        for item in items:
            f.write("{}\n".format(item))  # improved efficiency
    return full_path


def file_to_string(file_name):
    with open(file_name, 'r') as f:
        text = f.read()
    # delete original file
    os.remove(file_name)
    return text


def remove_commas(file_name):
    """Replaces commas with newlines in a file."""
    with open(file_name, 'r') as f:
        text = f.read()
        text = replace(text, ',', '\n')
    with open(file_name, 'w') as f:  # now writing
        f.write(text)
    return file_name


# def zip_results(fasta_output_path, csv_rbh_output_filename, csv_strict_output_filename, csv_ns_output_filename,
# output_path):
def zip_results(fasta_output_path, zip_list, output_path):
    """
    Receives a folder containing fasta sequences and a csv file, adds them all to zip.
    :param fasta_output_path:
    :param csv_rbh_output_filename:
    :param csv_strict_output_filename:
    :param csv_ns_output_filename:
    :param output_path:
    :return:
    """
    zip_file = join_folder(output_path, "output.zip")
    fastas = [join_folder(fasta_output_path, x) for x in os.listdir(fasta_output_path)]
    bname = os.path.basename  # for efficiency
    with zipfile.ZipFile(zip_file, mode='w') as zf:
        # adding all fasta files
        for fasta in fastas:
            zf.write(fasta, bname(fasta))
        # zf.write(csv_file_path, os.path.basename(csv_file_path))  # add csv file
        # add csv files
        for f_to_zip in zip_list:
            zf.write(f_to_zip, bname(f_to_zip))
        # zf.write(csv_rbh_output_filename, bname(csv_rbh_output_filename))  # add csv file
        # zf.write(csv_strict_output_filename, bname(csv_strict_output_filename))  # add csv file
        # zf.write(csv_ns_output_filename, bname(csv_ns_output_filename))  # add csv file
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
    """Return the file length in lines."""
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


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


def cleanup(path, storage_folder, run_id):
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
    aliastool_command = ["/usr/bin/blast/blastdb_aliastool", "-gilist", gi_file_path, "-db", big_db, "-dbtype", "prot",
                         "-out", target_db]
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


AWS_ACCESS_KEY = 'AKIAISOVBXCFDI3V4XKA'
AWS_SECRET_KEY = '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'
os.environ['AWS_ACCESS_KEY'] = AWS_ACCESS_KEY
os.environ['AWS_SECRET_KEY'] = AWS_SECRET_KEY


def get_s3_client(aws_access_key, aws_secret_key):
    # Hard coded strings as credentials, not recommended.
    print("Using access key: {0} and secret key: {1}".format(aws_access_key, aws_secret_key))
    return boto3.client('s3',
                        region_name='eu-central-1',
                        aws_access_key_id=aws_access_key,
                        aws_secret_access_key=aws_secret_key,
                        config=botocore.client.Config(signature_version='s3v4'))


def generate_download_link(user_id, fname, aws_access_key, aws_secret_key, expires=604800):
    """Generates S3 download link that expires after 1 week."""
    session = botocore.session.get_session()
    session.set_credentials(aws_access_key, aws_secret_key)
    client = session.create_client('s3',
                                   region_name='eu-central-1',
                                   aws_access_key_id=aws_access_key,
                                   aws_secret_access_key=aws_secret_key,
                                   config=botocore.client.Config(signature_version='s3v4')
                                   )
    presigned_url = client.generate_presigned_url('get_object', Params={'Bucket': 'recblastdata',
                                                                        'Key': '{}/{}'.format(user_id, fname)},
                                                  ExpiresIn=expires)
    return presigned_url


# TODO: document
# Viz functions:

def melt(df):
    species_columns = [x for x in df.columns if x != 'gene_name']
    melted_df = pd.melt(df, id_vars=['gene_name'], value_vars=species_columns, var_name='Species', value_name='orthologues')
    melted_df.columns = ['Gene Name', 'Species', 'Orthologues']
    # species list
    species = sorted(species_columns)
    # genes list
    genes = sorted(melted_df['Gene Name'].unique().tolist())
    return melted_df, species, genes


# receives melted_df
def create_swarmplot(df, path, title, cmap, genes, species):
    print("Creating swarmplot for {}".format(path))
    # TODO: change figure size
    output_path = os.path.dirname(path)
    output = join_folder(output_path, "%s_swarmplot.png" % title)
    fig = plt.figure(figsize=(16, 10), dpi=180)
    sns.swarmplot(x='Gene Name', y='Orthologues', hue='Species', order=genes, hue_order=species, data=df, palette=cmap)
    plt.ylabel("#Orthologues")
    plt.xlabel("Species")
    plt.ylim(0, )
    plt.suptitle(title, fontsize=16)
    plt.yticks(fontsize=10)
    plt.xticks(fontsize=10)
    plt.savefig(output)
    plt.close(fig)
    return output


# receives melted_df
def create_barplot(df, path, title, cmap, genes, species):
    print("Creating barplot for {}".format(path))
    # TODO: change figure size
    output_path = os.path.dirname(path)
    output = join_folder(output_path, "%s_barplot.png" % title)
    fig = plt.figure(figsize=(16, 10), dpi=180)
    sns.barplot(x='Gene Name', y='Orthologues', hue='Species', order=genes, hue_order=species, data=df, palette=cmap)
    plt.ylabel("#Orthologues")
    plt.xlabel("Species")
    plt.ylim(0, )
    plt.suptitle(title, fontsize=16)
    plt.yticks(fontsize=10)
    plt.xticks(fontsize=10)
    plt.savefig(output)
    plt.close(fig)
    return output


# receives melted_df
def create_barplot_orthologues_by_species(df, path, title, cmap, genes, species):
    print("Creating barplot_species for {}".format(path))
    # TODO: change figure size
    output_path = os.path.dirname(path)
    output = join_folder(output_path, "%s_barplot_byspecies.png" % title)
    fig = plt.figure(figsize=(16, 10), dpi=180)
    sns.barplot(x='Species', y='Orthologues', hue='Gene Name', order=species, hue_order=genes, data=df, palette=cmap)
    plt.ylabel("#Orthologues")
    plt.xlabel("Species")
    plt.ylim(0, )
    plt.suptitle(title, fontsize=16)
    plt.yticks(fontsize=10)
    plt.xticks(fontsize=10)
    plt.savefig(output)
    plt.close(fig)
    return output


def create_barplot_sum(df, path, title, cmap, genes, species):
    print("Creating barplot_sum for {}".format(path))
    # TODO: change figure size
    output_path = os.path.dirname(path)
    output = join_folder(output_path, "%s_barplot_sum.png" % title)
    fig = plt.figure(figsize=(16, 10), dpi=180)
    sns.barplot(x='Species', y='Orthologues', estimator=sum, ci=None, order=species, hue_order=genes, data=df, palette=cmap)
    plt.ylabel("#Orthologues")
    plt.xlabel("Species")
    plt.ylim(0, )
    plt.suptitle(title, fontsize=16)
    plt.yticks(fontsize=10)
    plt.xticks(fontsize=10)
    plt.savefig(output)
    plt.close(fig)
    return output


def create_heatmap(df, path, title, cmap):
    """

    :param df: a padnas DataFrame
    :param path: Path for the input and output file
    :param cmap: colormap
    :return:
    """
    print("Creating heatmap for {}".format(path))
    output_path = os.path.dirname(path)
    fig = plt.figure(figsize=(16, 10), dpi=180)
    plt.title(title, fontsize=16)
    sns.heatmap(df, annot=True, fmt="d", cmap=cmap)
    plt.yticks(fontsize=10)
    plt.xticks(fontsize=10)
    output = join_folder(output_path, "%s_heatmap.png" % title)
    plt.savefig(output)
    plt.close(fig)
    return output


def create_clustermap(df, path, title, cmap, col_cluster, dont_cluster):
    """

    :param df: a padnas DataFrame
    :param path: Path for the input and output file
    :param cmap: colormap
    :param col_cluster: Boolean - True/False
    :return:
    """
    print("Creating clustermap for {}".format(path))
    # TODO: change figure size
    output_path = os.path.dirname(path)
    output = join_folder(output_path, "%s_clustermap.png" % title)
    fig = plt.figure(figsize=(16, 10), dpi=180)
    if not dont_cluster:
        sns.clustermap(df, annot=True, col_cluster=col_cluster, fmt="d", cmap=cmap, linewidths=.5)
        # plt.suptitle(title, fontsize=16)
        plt.yticks(fontsize=10)
        plt.xticks(fontsize=10)
    plt.savefig(output)
    plt.close(fig)
    return output


# generate heatmap and clustermap
def generate_visual_graphs(csv_rbh_output_filename, csv_strict_output_filename, csv_ns_output_filename):
    """
    The function generates heatmap + clustermap for the output data.
    :param csv_rbh_output_filename:
    :param csv_strict_output_filename:
    :param csv_ns_output_filename:
    :return:
    """
    # reading as data_frame
    nonstrict_data = pd.read_csv(csv_ns_output_filename, index_col=0)
    strict_data = pd.read_csv(csv_strict_output_filename, index_col=0)
    rbh_data = pd.read_csv(csv_rbh_output_filename, index_col=0)

    # transpose data
    df_nonstrict = pd.DataFrame.transpose(nonstrict_data)
    df_strict = pd.DataFrame.transpose(strict_data)
    df_rbh = pd.DataFrame.transpose(rbh_data)

    # melt them!
    melt_df_nonstrict, genes_list, species_list = melt(df_nonstrict)
    melt_df_strict, genes_list, species_list = melt(df_strict)
    melt_df_rbh, genes_list, species_list = melt(df_rbh)

    # clustering enabler (( one is enough because all files contains the same amount of genes ))
    dont_cluster = False
    col_cluster = False
    if len(df_nonstrict.columns) > 2:
        col_cluster = True
    elif len(df_nonstrict) <= 2:
        # dont_cluster = True
        dont_cluster = False  # I removed it for now

    # create graph, (( title, cmap ))
    # visual outputs:
    viz_dict = dict()
    print("Creating heatmaps and clustermpaps")
    viz_dict['non_strict_heatmap'] = create_heatmap(df_nonstrict, csv_ns_output_filename, 'non_strict', "BuGn")
    viz_dict['non_strict_clustermap'] = create_clustermap(df_nonstrict, csv_ns_output_filename, 'non_strict', "PuBu",
                                                          col_cluster, dont_cluster)
    viz_dict['non_strict_barplot'] = create_barplot(melt_df_nonstrict, csv_ns_output_filename, 'non_strict', "BuGn",
                                                    genes_list, species_list)
    viz_dict['non_strict_barplot_2'] = create_barplot_orthologues_by_species(melt_df_nonstrict, csv_ns_output_filename,
                                                                             'non_strict', "BuGn",
                                                                             genes_list, species_list)
    viz_dict['non_strict_swarmplot'] = create_swarmplot(melt_df_nonstrict, csv_ns_output_filename, 'non_strict', "BuGn",
                                                        genes_list, species_list)
    viz_dict['non_strict_barplotsum'] = create_barplot_sum(melt_df_nonstrict, csv_ns_output_filename,
                                                           'non_strict', "BuGn", genes_list, species_list)
    # strict
    viz_dict['strict_heatmap'] = create_heatmap(df_strict, csv_strict_output_filename, 'strict', "Oranges")
    viz_dict['strict_clustermap'] = create_clustermap(df_strict, csv_strict_output_filename, 'strict', "YlOrRd",
                                                      col_cluster, dont_cluster)
    viz_dict['strict_barplot'] = create_barplot(melt_df_strict, csv_strict_output_filename, 'strict', "YlOrRd",
                                                genes_list, species_list)
    viz_dict['strict_barplot_2'] = create_barplot_orthologues_by_species(melt_df_strict, csv_strict_output_filename,
                                                                         'strict', "YlOrRd", genes_list, species_list)
    viz_dict['strict_swarmplot'] = create_swarmplot(melt_df_strict, csv_strict_output_filename,
                                                    'strict', "YlOrRd", genes_list, species_list)
    viz_dict['strict_barplotsum'] = create_barplot_sum(melt_df_strict, csv_strict_output_filename,
                                                       'strict', "YlOrRd", genes_list, species_list)
    # RBH
    viz_dict['RBH_heatmap'] = create_heatmap(df_rbh, csv_rbh_output_filename, 'RBH', "YlGnBu")
    viz_dict['RBH_clustermap'] = create_clustermap(df_rbh, csv_rbh_output_filename, 'RBH', "YlGnBu", col_cluster,
                                                   dont_cluster)
    viz_dict['RBH_barplot'] = create_barplot(melt_df_rbh, csv_rbh_output_filename, 'RBH', "YlGnBu",
                                             genes_list, species_list)
    viz_dict['RBH_barplot_2'] = create_barplot_orthologues_by_species(melt_df_rbh, csv_rbh_output_filename,
                                                                            'RBH', "YlGnBu", genes_list, species_list)
    viz_dict['RBH_swarmplot'] = create_swarmplot(melt_df_rbh, csv_rbh_output_filename,
                                                                            'RBH', "YlGnBu", genes_list, species_list)
    viz_dict['RBH_barplotsum'] = create_barplot_sum(melt_df_rbh, csv_rbh_output_filename,
                                                                            'RBH', "YlGnBu", genes_list, species_list)

    return viz_dict


# for efficiency
strip = str.strip
split = str.split
replace = str.replace
re_search = re.search
re_sub = re.sub
re_match = re.match
upper = str.upper
lower = str.lower
join_folder = os.path.join

email_template = 'templates/email_templates/email_template.html'
