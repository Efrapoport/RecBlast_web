# from email_module import email_status
import RecBlastAWS
import users
#
# Here we add methods we want to run in the worker
#

# # just temporarily
# def email_func(app_contact_email, run_name, run_id, email_string, template):
#     if email_status(app_contact_email, run_name, run_id, email_string, template):
#         return True


def run_recblast_web(values_from_web):
    """run recblast web (from RecBlastAWS) with values from app.py"""
    try:
        return RecBlastAWS.run_from_web(values_from_web)
    except Exception, e:
        users.delete_email(values_from_web[10])
        print("Unknown error: {}\nDeleting email.".format(e))
        raise e


def run_test_loop(number_of_times):
    for i in xrange(number_of_times):
        print i
