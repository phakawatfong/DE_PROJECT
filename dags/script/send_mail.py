import smtplib
import os.path
from datetime import datetime
from email.message import EmailMessage
from configparser import ConfigParser

## SETUP  parameter.

current_dir=os.getcwd()
# print("current_dir={}".format(current_dir))
config_dir=f"{current_dir}\\config.conf"
output_dir=f"{current_dir}\\output"
file_to_be_check = f"{output_dir}\\final_carsome.csv"

# datetime object containing current date and time
now = datetime.now()
# dd/mm/YY H:M:S
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

# path configuration.
def checkFileExists(path):
    check_file_status = os.path.isfile(path)
    return check_file_status

def get_config_dict(config_dir, param_dict):
    config = ConfigParser()
    config.read(config_dir)
    details_dict = dict(config.items(param_dict))
    return details_dict

def send_email(configuration_param, check_file_status, to_customer):
    msg = EmailMessage()

    if check_file_status == True:
        msg['Subject'] = 'carSome WebScraped : SUCCEED.'
        TEXT = f"carSome WebScraping Process, successfully run. on {dt_string}"
    else:
        msg['Subject'] = 'carSome WebScraped : FAILED.'
        TEXT = f"ERROR: Not found `final_carsome.csv` file in the {output_dir} directory."

    msg['From'] = configuration_param["app_user"]

    if to_customer.lower() == "y":
        msg['To'] = configuration_param["customer_mail"]
    else:
        msg['To'] = configuration_param["mail_receiver"]

    msg.set_content(TEXT)

    # creates SMTP session
    server = smtplib.SMTP(configuration_param["mail_host"], configuration_param["mail_port"])
    # start TLS for security
    server.starttls()
    server.login(configuration_param["app_user"], configuration_param["app_password"])
    server.send_message(msg)
    server.quit()


# Main Program
def main():
    print("##################### send_mail.py IS RUNNING #######################")
    to_customer = input("Send an email to the customer (Y/N) ? : ")

    # SETUP parameter.
    current_dir = os.getcwd()
    config_dir = os.path.join(current_dir, "config.conf")
    output_dir = os.path.join(current_dir, "output")
    file_to_be_check = os.path.join(output_dir, "final_carsome.csv")

    # datetime object containing current date and time
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

    CONFIG_KEY = "KIDS_GMAIL_PASSWORD"
    configuration_param = get_config_dict(config_dir, CONFIG_KEY)

    check_file = checkFileExists(file_to_be_check)
    send_email(configuration_param, check_file, to_customer)

if __name__ == "__main__":
    main()



