from email_module import email_status
#
# Here we add methods we want to run in the worker
#


def email_func(app_contact_email, run_name, run_id, email_string):
    if email_status(app_contact_email, run_name, run_id, email_string):
        return True

def part_one(param1, param2, param3):
    """

    :param param1:
    :param param2:
    :param param3:
    :return:
    """
    pass

def run_test_loop(number_of_times):
    for i in xrange(number_of_times):
        print i
