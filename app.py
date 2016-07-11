# import os
# from uuid import uuid4

from flask import Flask, render_template, redirect, url_for, request, flash, send_from_directory
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
import csv_transformer
import taxa_to_taxid
import users
# import RecBlastUtils
from taxa import get_name_by_value, get_value_by_name
from RecBlastUtils import *
import queue
from email_module import *
# import RecBlastAWS

os.environ['BLASTDB'] = "/blast/db"  # setting the $blastdb # check if it workswq

# UPLOAD_FOLDER = r'C:\Users\Efrat\PycharmProjects\recblast\uploaded_files\'  # TODO: change later
UPLOAD_FOLDER = ""
ALLOWED_EXTENSIONS = {'txt', 'csv'}

app = Flask(__name__, static_folder='public', static_url_path='')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['SECRET_KEY'] = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'


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
    """Send the job to queue."""
    user_email = value_list[10]
    run_name = value_list[9]
    run_id = value_list[11]
    # email_template = 'templates/email_templates/email_template.html'
    #  just in case it didn't find it from RecBlastUtils
    if email_status(user_email, run_name, run_id,
                    "Your job {0} is now on queue for running on RecBlast online!<br>"
                    "We will update you when your run starts.<br>"
                    "Thanks for using RecBlast!".format(run_name), email_template):
        print("email sent to {}".format(user_email))
    try:
        run_result = queue.run_recblast_on_worker(value_list)
        return run_result
        # sending to the backend main
        # return True
    except Exception, e:
        users.delete_email(value_list[10])
        users.delete_user_id_for_email(value_list[10])
        print("error: {}\nDeleting email.".format(e))
        print("Unknown error. Please report to recblast@gmail.com.")
        return False


def validate_data(value_list):
    """Validates and modifies value_list."""
    print value_list  # DEBUG
    error_list = []

    # check email
    email = value_list[9]

    if users.has_job_for_email(email):
        error_list.append("There's already a job running for this email: %s" % email)

    try:
        # check if files/lists are not empty and not too long:
        gene_file_path = value_list[5]
        max_allowed_genes = 10  # this is where we set the maximum allowed genes
        if exists_not_empty(gene_file_path):
            if file_len(gene_file_path) > max_allowed_genes:
                error_list.append(
                    "Genes provided exceed the maximum number of allowed genes: {}".format(max_allowed_genes))
        else:
            error_list.append("No gene list or gene file provided!")

        taxa_file = value_list[6]
        max_allowed_taxa = 10  # this is where we set the maximum allowed taxa
        if exists_not_empty(taxa_file):
            if file_len(taxa_file) > max_allowed_taxa:
                error_list.append(
                    "Genes provided exceed the maximum number of allowed taxa: {}".format(max_allowed_taxa))
        else:
            error_list.append("No taxa list or gene file provided!")

        # validate reference taxa!
        reference_taxa = value_list[7]
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
            origin_species = reference_taxa.capitalize()
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
        # is_csv = False  # by default
        # with open(gene_file_path, 'r') as gene_file:
        #     for line in gene_file:
        #         if line.find(',') != -1:
        #             is_csv = True
        #             break
        # if is_csv:
        #     csv_path = gene_file_path  # hope it's a good and valid file...
        #     error_list.append("It's csv, we don't support it now!")
        gene_file_path = remove_commas(gene_file_path)

        # else:  # if the csv is not provided, create it from a gene file
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
        taxa_file = remove_commas(taxa_file)
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
            taxa_list_file = ""
        # debug("Converted tax list to tax ID files, saved new list in: {}".format(taxa_list_file))

        # submit csv and taxa as lists
        if taxa_list_file == "":
            taxa_file_content = ""
        else:
            taxa_file_content = file_to_string(taxa_list_file)

        if csv_path == "":
            gene_file_content = ""
        else:
            gene_file_content = file_to_string(csv_path)

        new_value_list = [value_list[0], value_list[1], value_list[2], value_list[3], value_list[4], gene_file_content,
                          taxa_file_content, origin_species, org_tax_id, value_list[8], value_list[9], value_list[10],
                          value_list[11], value_list[12], value_list[13]]
        return error_list, new_value_list

    except Exception, e:  # if any error occurs:
        users.delete_email(email)  # remove the emails
        users.delete_user_id_for_email(email)  # remove the emails
        print("error: {}\nDeleting email.".format(e))
        return ["Unknown error. Please report to recblast@gmail.com."], []


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


def form_input_to_list(string_input):
    if not string_input:
        return []

    # Handle file fields
    if isinstance(string_input, FileStorage):
        string_input = string_input.stream.read()

    items = string_input.strip().split("\n")
    return [i.strip() for i in items]


# unique output page per user, displays one out of several options:
# 1. error (+ go back button)
# 2. success! check back soon, we also sent you an email
@app.route('/output', methods=['POST'])
def handle_data():
    email = request.form['email']
    user_id = users.user_id_for_email(email)
    try:
        user_ip = request.headers.get('X-Forwarded-For') or request.remote_addr  # get the user ip
    except:
        user_ip = 'localhost'  # pretty bad then
    run_name = request.form['run_name']
    reference_taxa = request.form['reference_taxa']

    taxa_list = list(set(form_input_to_list(request.files.get('taxons') or request.form.get('taxa_list'))))
    path_to_taxa = prepare_files(taxa_list, "taxons", user_id)

    gene_list = list(set(form_input_to_list(request.files.get('genes') or request.form.get('gene_list'))))
    path_to_genes = prepare_files(gene_list, "genes", user_id)

    evalue = request.form['evalue']
    back_evalue = request.form['back_evalue']
    identity = request.form['identity']
    coverage = request.form['coverage']
    string_similarity = request.form['string_similarity']

    value_list = [float(evalue), float(back_evalue), int(identity), int(coverage), float(string_similarity),
                  path_to_genes, path_to_taxa, reference_taxa, run_name, email, user_id, user_ip, taxa_list, gene_list]
    # message_list is a list of errors we need to pass back to the user
    # if the list is empty, we move on to the run. else, we redirect use back to the form.
    message_list, value_list = validate_data(value_list)

    # message_list.append("Your file is not in the right format")

    if message_list:  # means there are errors. later we might redirect the user to the form and
        return render_template("server.html", errors=message_list, user_id=user_id)

    elif not message_list:
        # temp_email_string = "Someone sent a new job on RecBlast online!\n" \
        #                 "email: {0}, run name: {1}, , run_id: {2}, species of origin: {3} (taxid: {4}), ip: {5}\n" \
        #                     "Started at: {6}".format(email, run_name, user_id, reference_taxa, value_list[8], user_ip,
        #                                              strftime('%H:%M:%S'))
        # new_value_list = ["recblast@gmail.com", run_name, user_id, temp_email_string]
        # success = send_job_to_backend(value_list)
        success = send_job_to_backend(value_list)  # temp
        if success:
            # flash('You successfully sent out a job!')
            message = "Your job was sent to the server successfully! You will receive an email with a link " \
                      "to check the progress of your job."

            users.set_has_job_for_email(email, True)
            return render_template("page.html", message=message)
        else:
            error = "There was an unknown error with you data. Please try again: "
            error += "\n\n Go back to the form and edit you data:<a href='server'>Home</a>"
            return render_template("server.html", errors=error, user_id=user_id)

    else:
        error = "Unknown error. Please try again"
        return render_template("server.html", errors=error, user_id=user_id)


@app.route('/results/<user_id>')
def results(user_id):
    download_path = users.get_result_by_user_id(user_id)
    return render_template('results.html', user_id=user_id, download_path=download_path)


@app.route('/flashio')
def flashio():
    flash('yo yo yo!')
    return 'ok'


@app.route('/BingSiteAuth.xml')
def BingSiteAuth():
    return render_template('BingSiteAuth.xml')


@app.route('/sitemap.xml')
def sitemap():
    return render_template('sitemap.xml')

if __name__ == "__main__":
    app.run(debug=True)
