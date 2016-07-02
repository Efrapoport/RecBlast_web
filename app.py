# import os
from uuid import uuid4

from flask import Flask, render_template, redirect, url_for, request, flash, send_from_directory
from werkzeug.utils import secure_filename

import csv_transformer
import taxa_to_taxid
import users
import utils
from taxa import get_name_by_value, get_value_by_name
from utils import *
import queue

UPLOAD_FOLDER = 'r"C:\Users\Efrat\PycharmProjects\recblast\uploaded_files\"'
ALLOWED_EXTENSIONS = {'txt', 'csv'}

app = Flask(__name__, static_folder='public', static_url_path='')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['SECRET_KEY'] = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'

try:
    ip = request.remote_addr
    print ip
except:
    ip = "localhost"
    print ip


@app.route('/index')
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/documentation')
def documentation():
    return render_template('documentation.html')


@app.route('/about')
def about():
    return render_template('about.html')


@app.route('/downloads')
def downloads():
    return render_template('downloads.html')


@app.route('/imgs/<path:filename>')
def serve_images(filename):
    return send_from_directory('public/imgs',
                               filename)


@app.route('/css/<path:filename>')
def serve_css(filename):
    return send_from_directory('public/css',
                               filename)


@app.route('/server')
def server():
    return render_template("server.html")


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


def send_job_to_backend(value_list):
    try:
        # sending to the backend main
        return True
    except:
        return False


def validate_data(value_list):
    print value_list
    error_list = []

    # check email
    email = value_list[9]

    if users.has_job_for_email(email):
        error_list.append("There's already a job running for this email: %s" % email)

    # check if files/lists are not empty:
    gene_file_path = value_list[5]

    if not exists_not_empty(gene_file_path):
        error_list.append("No gene list or gene file provided!")

    taxa_file = value_list[6]
    if not exists_not_empty(taxa_file):
        error_list.append("No taxa list or gene file provided!")

    # taxa database
    # script folder
    script_folder = "/groups/igv/moranne/scripts/RecBlast_AWS/"  # TODO: Change to a fixed path

    reference_taxa = value_list[7]
    # validate reference taxa!
    # processing original species
    if is_number(reference_taxa):  # already a tax_id
        org_tax_id = reference_taxa
        try:

            origin_species = get_name_by_value(org_tax_id).capitalize()
        except KeyError:
            error_list.append("Unknown tax id: {}!".format(org_tax_id))
            origin_species = ""
            # debug("Tax id given: {0}, which is {1}, no need to convert.".format(org_tax_id, origin_species))
    else:  # it's a tax name
        origin_species = reference_taxa
        # convert it to tax id and write it to a file
        try:
            org_tax_id = get_value_by_name(origin_species)
        except KeyError:
            error_list.append("Unknown taxon name: {}!".format(origin_species))
            org_tax_id = ""
            # debug("Tax name given: {0}, which is tax_id {1}, converted. Saved original tax_id to {2}".format(
            #     origin_species, org_tax_id, original_taxa_file))
    # left with (org_tax_id, origin_species)

    #   validate gene list
    # parsing the genes_csv if it's a csv, and transforming it if it's a gene list file
    is_csv = False  # by default
    with open(gene_file_path, 'r') as gene_file:
        for line in gene_file:
            if line.find(',') != -1:
                is_csv = True
                break
    if is_csv:
        csv_path = gene_file_path  # hope it's a good and valid file...
        # TODO: don't do this now
        error_list.append("It's csv, we don't support it now!")
    else:  # if the csv is not provided, create it from a gene file
        if org_tax_id == "":
            error_list.append("The gene_list could not be converted. Please check and upload a valid file.")
            csv_path = ""
        else:
            conversion_result = csv_transformer.gene_file_to_csv(gene_file_path, org_tax_id)
            if conversion_result[0]:
                csv_path = conversion_result[1]  # quits if no input could be converted
            else:
                error_list.append("The gene_list could not be converted. Please check and upload a valid file.")
                csv_path = ""

    # validate taxa list
    # converting taxa names list
    (taxa_list_file, bad_tax_list, conversion_succeeded) = taxa_to_taxid.convert_tax_to_taxid(taxa_file)
    if conversion_succeeded:
        if bad_tax_list:
            taxa_warning = "Bad taxa names found in the file provided: {}.Ignoring them.".format(
                "\n".join(bad_tax_list))
            flash(taxa_warning)
    else:
        taxa_error = "No valid taxa names or IDs found in the list or file provided: {}.Ignoring them.".format(
            "\n".join(bad_tax_list))
        error_list.append(taxa_error)
    # debug("Converted tax list to tax ID files, saved new list in: {}".format(taxa_list_file))

    new_value_list = [value_list[0], value_list[1], value_list[2], value_list[3], value_list[4], csv_path,
                      taxa_list_file, origin_species, org_tax_id, value_list[8], value_list[9], value_list[10],
                      value_list[11]]
    return error_list, new_value_list


def upload(request):
    if request.method == 'POST':
        uploaded_files = request.files.getlist("taxons")
        print uploaded_files
        uploaded_file = request.files['file']
        if uploaded_file and allowed_file(uploaded_file.filename):
            filename = secure_filename(uploaded_file.filename)
            print filename
            uploaded_file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return redirect(url_for('uploaded_file',
                                    filename=filename))
    return


# unique output page per user, displays one out of several options:
# 1. error (+ go back button)
# 2. success! check back soon, we also sent you an email
@app.route('/output', methods=['POST'])
# @app.route('/output', methods=['POST'])
def handle_data():
    email = request.form['email']
    user_id = users.user_id_for_email(email)
    run_name = request.form['run_name']
    reference_taxa = request.form['reference_taxa']

    taxa_list = request.form['taxa_list']
    taxa_list = taxa_list.split("\n")
    taxa_list = [i.strip() for i in taxa_list]
    path_to_taxa = utils.prepare_files(taxa_list, "taxons", user_id)

    gene_list = (request.form['gene_list'])
    gene_list = gene_list.split("\n")
    gene_list = [i.strip() for i in gene_list]
    path_to_genes = utils.prepare_files(gene_list, "genes", user_id)

    # to do: FIND A WAY TO UPLOAD FILES~~
    # upload(request)
    # file = request.form.files['taxa_file']
    # file.save(os.path.join(app.config['UPLOAD_FOLDER'], file))

    evalue = request.form['evalue']
    back_evalue = request.form['back_evalue']
    identity = request.form['identity']
    coverage = request.form['coverage']
    string_similarity = request.form['string_similarity']

    value_list = [float(evalue), float(back_evalue), int(identity), int(coverage), float(string_similarity),
                  path_to_genes, path_to_taxa, reference_taxa, run_name, email, user_id, ip]
    # message_list is a list of errors we need to pass back to the user
    # if the list is empty, we move on to the run. else, we redirect use back to the form.
    message_list, value_list = validate_data(value_list)

    # message_list.append("You forgot something")
    # message_list.append("Your file is not in the right format")

    if message_list:  # means there are errors. later we might redirect the user to the form and
        return render_template("server.html", errors=message_list, user_id=user_id)

    elif not message_list:
        success = send_job_to_backend(value_list)
        if success:
            flash('You successfully sent out a job!')
            message = "Your job was sent to the server successfully! You will receive an email with a link" \
                      "and you will be able to check the progress of your job"

            users.set_has_job_for_email(email, True)
            return render_template("page.html", message=message)
        else:
            error = "There was an unknown error with you data. Please try again: "
            error += "\n\n Go back to the form and edit you data:<a href='server'>Home</a>"
            return render_template("server.html", errors=error, user_id=user_id)

    else:
        error = "unkown error. try again"
        return render_template("server.html", errors=error, user_id=user_id)


@app.route('/results/<user_id>')
def results(user_id):
    return render_template('results.html')


@app.route('/run_queue')
def run_queue():
    queue.run_part_one(100)
    return 'ok'


if __name__ == "__main__":
    app.run(debug=True)
