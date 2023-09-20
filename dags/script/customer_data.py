import requests
import json
import pandas as pd
import os

def getCustomerAPI(url, CONVERSION_RATE):
    CONVERSION_RATE=CONVERSION_RATE
    url_link = url
    response_API = requests.get(url_link)

    data = response_API.text
    jsonStr = json.loads(data)
    df = pd.DataFrame.from_dict(jsonStr)

    # extract only needed column(s).
    customer_data_df=df.filter(['first_name', 'last_name', 'email', 'phone', 'gender', 'age', 'spent', 'is_married'], axis=1)

    ## Let's assume that spent column infers to customer salary for further use. 
    customer_data_df.rename(columns ={'spent' : 'salary_per_day_usd'}, inplace=True)

    customer_data_df['salary_in_THB'] = customer_data_df['salary_per_day_usd'] * CONVERSION_RATE

    return customer_data_df

def customerTobeSendEmail(df, output_file_path, salary_threshold):
    # EDA statement /  get data of the customers which has salary more than 30,000 THB / months
    high_salary_cust = df[df['salary_in_THB'] > salary_threshold]

    #### print statement.
    print(high_salary_cust[['first_name', 'email', 'salary_in_THB']].count())

    ## save  customers which has salary more than 33,000 THB / months into csv file for further use.
    high_salary_cust.to_csv(output_file_path, index=False)
    
    s=""
    for email in high_salary_cust['email']:
        s= s + f"{email} ,"
    # remove "," at the end of the string.
    list_of_email = s[:-1]
    return list_of_email

def writeEmailfile(output_file_path, list_of_email):
    search_word='CUSTOMER_MAIL='
    with open(output_file_path, 'r') as file:
        lines = file.readlines()

    # Find the line containing the search word and replace it with the new text
    replaced = False  # Flag to track if line has been replaced
    for i in range(len(lines)):
        if search_word in lines[i]:
            lines[i] =  search_word + list_of_email + '\n'
            replaced = True  # Flag to track if line has been replaced
            break

    # If line was not found, append it to the end of the file
    if not replaced:
        lines.append('\n' + search_word + list_of_email + '\n')

    # Write the modified lines back to the file
    with open(output_file_path, 'w') as file:
        file.writelines(lines)

#### set up parameter 
def main():
    call_send_mail = input("Do you want to call send_mail.py (Y/N) ? : ")

    current_dir = os.getcwd()
    customer_email_file = os.path.join(current_dir, "config.conf")
    output_dir = os.path.join(current_dir, "output")
    output_etl_file = os.path.join(output_dir, "high_salary_cust.csv")

    url = "https://api.slingacademy.com/v1/sample-data/files/customers.json"
    conversion_rate = float(34.62)  # as of 2023-07-16
    salary_threshold = 33000

    customer_data_df = getCustomerAPI(url, conversion_rate)
    if customer_data_df is not None:
        list_of_email_txt = customerTobeSendEmail(customer_data_df, output_etl_file, salary_threshold)
        writeEmailfile(customer_email_file, list_of_email_txt)

    if call_send_mail.lower() == "y":
        try:
            import send_mail as send_mail_script
            # Call the main function of the send_mail script if available
            if hasattr(send_mail_script, 'main') and callable(getattr(send_mail_script, 'main')):
                send_mail_script.main()
            else:
                print("send_mail.py does not have a main function.")
        except ImportError:
            print("Error importing send_mail.py")
    else:
        exit(0)

if __name__ == "__main__":
    main()