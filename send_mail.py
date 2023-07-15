import smtplib
import os.path
from datetime import datetime
from email.message import EmailMessage
from configparser import ConfigParser

## SETUP  parameter.

# datetime object containing current date and time
now = datetime.now()
# dd/mm/YY H:M:S
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

# path configuration.
path = './output/final_carsome.csv'
check_file = os.path.isfile(path)

def get_config_dict():
    config = ConfigParser()
    config.read("C:\\Users\\asus\\Desktop\\Kids\\Kids_Programming_Project\\de_car_proj\\config.conf")
    details_dict = dict(config.items("KIDS_GMAIL_PASSWORD"))
    return details_dict

configuration_param = get_config_dict()

msg = EmailMessage()

if check_file == True:
    msg['Subject'] = 'carSome WebScraped : SUCCEED.'
    TEXT = f"carSome WebScraping Process, successfully run. on {dt_string}"
else:
    msg['Subject'] = 'carSome WebScraped : FAILED.'
    TEXT = f"ERROR: Not found `final_carsome.csv` file in the {os.path}\output\ directory."

msg['From'] = configuration_param["app_user"]
msg['To'] = configuration_param["app_user"]
msg.set_content(TEXT)

# creates SMTP session
server = smtplib.SMTP(configuration_param["mail_host"], configuration_param["mail_port"])
# start TLS for security
server.starttls()
server.login(configuration_param["app_user"], configuration_param["app_password"])
server.send_message(msg)
server.quit()




