import logging
import os

import httplib2
import pymysql
from apiclient import discovery
from datetime import datetime
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse

    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    import argparse

    flags = None

logging.basicConfig(filename='import_naics_project.log', filemode='w', level=logging.INFO)
start_time = datetime.now()

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'NAICS Code Project'

logging.info("SCOPES: %s", SCOPES)


def get_google_credentials():
    credential_dir = os.path.dirname(os.path.abspath(__file__))

    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'sheets_credentials.json')
    print "Google Sheets Connection Established for Credentials"
    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        # print('Storing credentials to ' + credential_path)
    return credentials


def get_google_sheet_properties(spreadsheet_id, sheet_title):
    print sheet_title
    properties = service.spreadsheets().get(spreadsheetId=spreadsheet_id)
    response = properties.execute()
    # sheet_title_for_backup = "Review Set"
    # sheet_title_for_format = "Review Set (Dev)"
    sheets = response['sheets']
    # print sheets
    for index, item in enumerate(response['sheets']):
        all_sheets_id = sheets[index].get("properties", {}).get("sheetId", "sheet_id not found")
        all_sheets_title = sheets[index].get("properties", {}).get("title", "sheet_title not found")
        print index, " : ", all_sheets_title, " : ", all_sheets_id
    print "\n"

    gp_sheet_id = int()
    gp_sheet_title = str()
    # Return all Sheet IDs and Titles
    for index, item in enumerate(response['sheets']):
        gp_sheet_id = sheets[index].get("properties", {}).get("sheetId", "sheet_id not found")
        gp_sheet_title = sheets[index].get("properties", {}).get("title", "sheet_title not found")
        print index, " : ", gp_sheet_title, " : ", gp_sheet_id
        if gp_sheet_title == sheet_title:
            gp_sheet_id = sheets[index].get("properties", {}).get("sheetId", "sheet_id not found")
            gp_sheet_title = sheets[index].get("properties", {}).get("title", "sheet_title not found")
            break
        if gp_sheet_title == sheet_title:
            gp_sheet_id = sheets[index].get("properties", {}).get("sheetId", "sheet_id not found")
            gp_sheet_title = sheets[index].get("properties", {}).get("title", "sheet_title not found")
            break
    print (gp_sheet_id, gp_sheet_title)
    return gp_sheet_id, gp_sheet_title


def update_google_sheet_properties(spreadsheet_id, target_sheet_id, title):
    properties = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": target_sheet_id,
                        "title": title,
                    },
                    "fields": "title",
                }
            }
        ]
    }

    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=properties)
    response = request.execute()
    print response


def create_backup_sheet(spreadsheet_id, sheet_id, batch_id):
    destination_id = {
        'destination_spreadsheet_id': spreadsheet_id,
    }
    request = service.spreadsheets().sheets().copyTo(spreadsheetId=spreadsheet_id,
                                                     sheetId=sheet_id, body=destination_id)
    response = request.execute()
    print "\n", response, "\n"
    logging.info("Google Sheets Connection Established for Create Backup Sheet")
    this_backup_sheet_id = response['sheetId']
    print "Update Sheet tab title"
    update_google_sheet_properties(spreadsheet_id, this_backup_sheet_id, "Review Set (Backup)")
    this_backup_spreadsheet_id = spreadsheet_id
    this_backup_sheet_title = "Review Set (Backup)"
    print (batch_id,
           this_backup_spreadsheet_id,
           this_backup_sheet_id,
           this_backup_sheet_title)

    print "MySQL Connection Established for Spreadsheet Properties: \n"
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    cur = sql_conn.cursor()
    sql_query = """ 
                    INSERT INTO naics_6_properties
                    (batch_id, spreadsheet_id, backup_sheet_id, backup_sheet_title)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    batch_id = %s,
                    spreadsheet_id = %s,
                    backup_sheet_id = %s,
                    backup_sheet_title = %s
                """

    data_set = (batch_id, this_backup_spreadsheet_id, this_backup_sheet_id, this_backup_sheet_title,
                batch_id, this_backup_spreadsheet_id, this_backup_sheet_id, this_backup_sheet_title)

    cur.execute(sql_query, data_set)
    sql_conn.commit()
    cur.close()
    sql_conn.close()


def delete_backup_sheet(spreadsheet_id):
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    print "MySQL Connection Established for last backup sheet properties"
    cur = sql_conn.cursor()

    sql_query = """
                    SELECT backup_sheet_id
                    FROM naics_6_properties
                """
    cur.execute(sql_query)
    results = cur.fetchall()
    cur.close()
    sql_conn.close()

    target_sheet_id = int()
    for row in results:
        print "%s" % (row[0])
        target_sheet_id = "%s" % (row[0])
    print "Delete backup sheet with Google Sheets API"
    update_values = {
        "requests": [
            {
                "deleteSheet": {
                    "sheetId": target_sheet_id
                }
            }
        ]
    }

    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=update_values)
    response = request.execute()
    return response


def get_google_sheets_data(spreadsheet_id, range_name):
    request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name)
    response = request.execute()
    logging.info("Google Sheets Connection Established for Get Sheet Data")
    if response.get('values') is not None:
        the_data = response.get('values')
    else:
        the_data = False
    return the_data


def parse_google_sheets_ecids(data):
    all_data_rows = []
    ecid_list_ready = []

    if len(data) != 0:
        for num, row in enumerate(data):
            if row[6] == 'correct' or row[6] == 'wrong':
                all_data_rows.insert(num, row)
                ecid_list_ready.insert(num, row[0])

    # print all_data_rows
    return ecid_list_ready


def parse_mysql_ecids(data):
    sql_ecids = []
    for num, item in enumerate(data):
        sql_ecids.insert(num, str(item))
    sql_list = "'" + "','".join(sql_ecids) + "'"
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    print "MySQL Connection Established"
    cur = sql_conn.cursor()
    sql_query = "SELECT ecid FROM naics_6_complete WHERE ecid IN (" + sql_list + ")"
    cur.execute(sql_query)
    results = cur.fetchall()
    cur.close()
    sql_conn.close()
    return results


def compare_ecid_results(sheets_data, mysql_data):
    # TODO:
    if len(sheets_data) == len(mysql_data):
        return True
    else:
        return False


def parse_google_sheets_data(data):
    data_list_ready = []
    data_list_not_ready = []
    data_list_invalid = []
    # print (len(data))
    if len(data) != 0:
        for num, row in enumerate(data):
            # print (len(row))
            if len(row) == 17:
                # print ("{} : {}".format(num, row))
                if row[6] == 'correct' or row[6] == 'wrong':
                    data_list_ready.insert(num, row)
                elif row[6] == 'review':
                    data_list_not_ready.insert(num, row)
                elif row[6] == 'invalid':
                    data_list_invalid.insert(num, row)
                else:
                    continue
            elif len(row) < 17:
                continue
    else:
        return 0

    data_list_ready_count = len(data_list_ready)
    data_list_not_ready_count = len(data_list_not_ready)
    data_list_invalid_count = len(data_list_invalid)
    return data_list_ready, data_list_ready_count, data_list_not_ready, data_list_not_ready_count, data_list_invalid, \
           data_list_invalid_count


def copy_paste_format(spreadsheet_id, type_num, sheet_title, s_start_column_index, s_end_column_index,
                      p_start_row_index,
                      p_end_row_index, p_start_column_index, p_end_column_index):
    cp_sheet_id = get_google_sheet_properties(spreadsheet_id, sheet_title)[0]
    print cp_sheet_id
    print (spreadsheet_id, type_num, s_start_column_index, s_end_column_index, p_start_row_index,
           p_end_row_index, p_start_column_index, p_end_column_index)

    update_values = {}
    if type_num == 1:
        update_values = {
            'requests': [
                {
                    'copyPaste': {
                        'source': {
                            'sheetId': int(cp_sheet_id),
                            'startRowIndex': 1,
                            'endRowIndex': 2,
                            'startColumnIndex': s_start_column_index,
                            'endColumnIndex': s_end_column_index,
                        },
                        'destination': {
                            'sheetId': int(cp_sheet_id),
                            'startRowIndex': p_start_row_index,
                            'endRowIndex': p_end_row_index,
                            'startColumnIndex': p_start_column_index,
                            'endColumnIndex': p_end_column_index,
                        },
                        'pasteType': 'PASTE_FORMULA',
                    }
                }
            ]
        }

    if type_num == 2:
        update_values = {
            'requests': [
                {
                    'copyPaste': {
                        'source': {
                            'sheetId': int(cp_sheet_id),
                            'startRowIndex': 1,
                            'endRowIndex': 2,
                            'startColumnIndex': s_start_column_index,
                            'endColumnIndex': s_end_column_index,
                        },
                        'destination': {
                            'sheetId': int(cp_sheet_id),
                            'startRowIndex': p_start_row_index,
                            'endRowIndex': p_end_row_index,
                            'startColumnIndex': p_start_column_index,
                            'endColumnIndex': p_end_column_index,
                        },
                        'pasteType': 'PASTE_FORMULA',
                    }
                },
                {
                    'copyPaste': {
                        'source': {
                            'sheetId': int(cp_sheet_id),
                            'startRowIndex': 1,
                            'endRowIndex': 2,
                            'startColumnIndex': s_start_column_index + 4,
                            'endColumnIndex': s_end_column_index + 11,
                        },
                        'destination': {
                            'sheetId': int(cp_sheet_id),
                            'startRowIndex': p_start_row_index,
                            'endRowIndex': p_end_row_index,
                            'startColumnIndex': p_start_column_index + 4,
                            'endColumnIndex': p_end_column_index + 11,
                        },
                        'pasteType': 'PASTE_FORMULA',
                    }
                }
            ]
        }

    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=update_values)
    response = request.execute()
    print response


def column_data_format(sheet_id, sheet_title, row_index):
    tf_sheet_id = get_google_sheet_properties(sheet_id, sheet_title)[0]
    update_values = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": int(tf_sheet_id),
                        "startRowIndex": 1,
                        "endRowIndex": row_index,
                        "startColumnIndex": 3,
                        "endColumnIndex": 4,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "TEXT",
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        ]
    }
    request = service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=update_values)
    response = request.execute()
    print response


def calculate_google_sheets_data(data):
    data_list = []
    for num, row in enumerate(data):
        if len(row[0]) > 0:
            # print ("{} : {}".format(num, row[0]))
            data_list.insert(num, row[0])

    data_list_count = len(data_list)

    print "Google Sheets Row Count: "
    print data_list_count
    return data_list_count


def update_google_sheets_data(spreadsheet_id, range_name, data_values):
    print data_values

    body = {
        'values': data_values
    }
    request = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption='USER_ENTERED', body=body)
    print "Google Sheets Connection Established for Update Sheet Data"
    print 'This data was created : '
    print request
    response = request.execute()
    print "\nGoogle Sheets API response:"
    print response
    print "\n"

    updated_range = response['updatedRange']
    print "Response data from update_google_sheets_data"
    print updated_range, "\n"
    updated_range_split = updated_range.split('!')
    updated_range_label = str(updated_range_split[0][1:][:-1])
    updated_range_notation = updated_range_split[1]
    updated_range_notation_split = updated_range_notation.split(':')

    updated_range_start = updated_range_notation_split[0]
    updated_range_start_row = updated_range_start[0]
    # updated_range_start_col = updated_range_start[1]

    updated_range_end = updated_range_notation_split[1]
    updated_range_end_col = updated_range_end[1:]

    updated_range_new_start_row = int(updated_range_end_col) + 1
    updated_range_start_final = updated_range_start_row + str(updated_range_new_start_row)
    updated_range_string = updated_range_label + '!' + updated_range_start_final

    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    data_values = result.get('values', [])

    if not data_values:
        print('No data found in data_values.')
    else:
        print('Data found in data_values')
        for this_row in data_values:
            # print data set containing the values written to the sheet.
            print('%s' % this_row)
    return updated_range_new_start_row, updated_range_string


def append_google_sheets_data(sheet_id, range_name, data_values):
    spreadsheet_id = sheet_id

    body = {
        'values': data_values
    }

    request = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption='RAW', body=body)
    print "Google Sheets Connection Established for Append Sheet Data"
    print 'This data was created : '
    print request
    response = request.execute()
    print response

    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    data_values = result.get('values', [])

    if not data_values:
        print('No data found in data_values.')
    else:
        print('Data found in data_values')
        for this_row in data_values:
            # print data set containing the values written to the sheet.
            print('%s' % this_row)


def clear_google_sheets_data(sheet_id, range_name):
    print "Google Sheets Connection Established for Clear Sheet Data"
    # Clear the data from the target sheet
    clear_values_request_body = {}
    request = service.spreadsheets().values().clear(spreadsheetId=sheet_id, range=range_name,
                                                    body=clear_values_request_body)
    response = request.execute()
    print 'This data was cleared : '
    print response


def get_mysql_source(naics_code, limit):
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    print "MySQL Connection Established for NAICS Code: " + naics_code + " in naics_6_source"
    cur = sql_conn.cursor()

    sql_query = """
                    SELECT ecid, company_name, company_domain, naics_6_source
                    FROM naics_6_source WHERE naics_6_source = '""" + naics_code + """'
                    AND naics_6_status IS NULL LIMIT """ + limit + """
                """
    cur.execute(sql_query)
    results = cur.fetchall()
    cur.close()
    sql_conn.close()

    print "NAICS Source Data"
    converted_results = []
    for num, tup in enumerate(list(results)):
        # print ("{} : {}".format(num, tup))
        converted_results.insert(num, tup)
    results_count = len(converted_results)
    return converted_results, results_count


def update_mysql_source(sheet_data, batch_id):
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    print "MySQL Connection Established for Update: naics_6_source"
    cur = sql_conn.cursor()
    sql_query = """ 
                    UPDATE naics_6_source
                    SET naics_6_status = 'Complete', batch_id = '""" + str(batch_id) + """'
                    WHERE ecid = %s 
                """

    for row in sheet_data:
        data_set = (row[0])
        cur.execute(sql_query, data_set)

    sql_conn.commit()
    cur.close()
    sql_conn.close()


def get_mysql_complete(batch_id):
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    print "MySQL Connection Established for Batch ID: naics_6_complete"
    cur = sql_conn.cursor()

    sql_query = """
                    SELECT ecid FROM naics_6_complete WHERE batch_id = '""" + str(batch_id) + """'
                """
    cur.execute(sql_query)
    results = cur.fetchall()
    cur.close()
    sql_conn.close()
    print results
    return results


def calculate_mysql_complete(batch_id):
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    print "MySQL Connection Established for Record Count: naics_6_complete"
    cur = sql_conn.cursor()
    sql_query = """
                    SELECT * FROM naics_6_complete WHERE batch_id = '""" + str(batch_id) + """'
                """
    cur.execute(sql_query)
    # determine length of results set and add one if not divisible by api batch limit
    num_rows = cur.rowcount
    cur.close()
    sql_conn.close()
    print "MySQL Table Row Count: "
    print num_rows
    return num_rows


def update_mysql_complete(sheet_data, batch_id, data_set_type):
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    print "MySQL Connection Established for Update: naics_6_complete"
    cur = sql_conn.cursor()
    sql_query_completed = """ 
                    INSERT INTO naics_6_complete
                         (ecid,
                         company_name,
                         company_domain,
                         naics_6_source,
                         company_website,
                         naics_classification_label,
                         verification,
                         naics_6_confirm,
                         naics_6_automatic,
                         naics_code_label,
                         naics_code_url,
                         naics_code_description,
                         naics_code_2_digit,
                         naics_code_3_digit,
                         naics_code_4_digit,
                         naics_code_5_digit,
                         naics_code_6_digit,
                         batch_id)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                         ON DUPLICATE KEY UPDATE
                         ecid = %s,
                         company_name = %s,
                         company_domain = %s,
                         naics_6_source = %s,
                         company_website = %s,
                         naics_classification_label = %s,
                         verification = %s,
                         naics_6_confirm = %s,
                         naics_6_automatic = %s,
                         naics_code_label = %s,
                         naics_code_url = %s,
                         naics_code_description = %s,
                         naics_code_2_digit = %s,
                         naics_code_3_digit = %s,
                         naics_code_4_digit = %s,
                         naics_code_5_digit = %s,
                         naics_code_6_digit = %s,
                         batch_id = %s 
                """
    if data_set_type == "completed":
        for row in sheet_data:
            data_set = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                        row[10], row[11], row[12], row[13], row[14], row[15], row[16], str(batch_id),
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                        row[10], row[11], row[12], row[13], row[14], row[15], row[16], str(batch_id))
            cur.execute(sql_query_completed, data_set)

        success = sql_conn.commit()
        cur.close()
        sql_conn.close()

        print success

    if data_set_type == "invalid":
        for row in sheet_data:
            data_set = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                        row[10], row[11], row[12], row[13], row[14], row[15], row[16], str(batch_id),
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                        row[10], row[11], row[12], row[13], row[14], row[15], row[16], str(batch_id))
            cur.execute(sql_query_completed, data_set)

        success = sql_conn.commit()
        cur.close()
        sql_conn.close()

        print success


def calculate_mysql_naics_counts(naics_code):
    sql_conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="Everstring411!",
                               charset="utf8",
                               db="data_operations")

    print "MySQL Connection Established for NAICS Code Count: naics_6_complete"
    cur = sql_conn.cursor()

    sql_query_1 = """
                    SELECT ecid FROM naics_6_complete 
                    WHERE naics_6_automatic = '""" + naics_code + """'
                """

    cur.execute(sql_query_1)
    results = cur.fetchall()
    cur.close()
    sql_conn.close()

    sql_query_1_results = len(results)
    print sql_query_1_results

    if sql_query_1_results < 30:
        print "Only found " + str(sql_query_1_results) + " records."
    else:
        print "Found " + str(sql_query_1_results) + " records"


# Default variables for this data project
this_spreadsheet_id = '1qfENW2qeRSDnlw8Z4pvjgufnVGGnmb-1_vFQwdBmg0I'
# this_sheet_title = 'Review Set (Dev)'
# this_range_name = 'Review Set (Dev)!A3:Q1000'  # Development Sheet
# this_sheet_title = 'Review Set (Dev)'
this_range_name = 'Review Set (Test)!A3:Q1000'  # Test Sheet
this_sheet_title = 'Review Set (Test)'
# this_range_name = 'Review Set!A3:Q1000'  # Production Sheet
this_sheet_header = 2
# Used for testing new data loaded to Google Sheet
this_data_values = [
    ['100000981', 'Southwest Capital Bank', 'southwestcapital.com', '522110'],
    ['100002793', 'Cruise America Inc', 'cruiseamerica.com', '532120']
]

# Set ready state for production run to retrieve and load data from sources
# 0 = retrieve data for debugging/analysis prior to production run
# 1 = execute production run for retrieving data from Google Sheet and updating to MySQL
# 2 = execute Google Sheet clean up and data load from MySQL
ready_state = 0
# Update these variables for each production run for updating MySQL and Google Sheet
this_batch_id = 7
this_naics_code = '541110'
this_limit = '100'

# Core functions for production updates to MySQL and Google Sheet
valid_credentials = get_google_credentials()
http = valid_credentials.authorize(httplib2.Http())
discovery_url = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
service = discovery.build('sheets', 'v4', http=http,
                          discoveryServiceUrl=discovery_url,
                          cache_discovery=False)

if ready_state == 0:
    logging.info("Ready State %s", ready_state)
    # delete_backup_sheet(this_spreadsheet_id)
    # backup_sheet_id = get_google_sheet_properties(this_spreadsheet_id, this_sheet_title)[0]
    # create_backup_sheet(this_spreadsheet_id, backup_sheet_id, this_batch_id)

    # Used for testing getting data from Google Sheet
    # this_data = get_google_sheets_data(this_spreadsheet_id, this_range_name)
    # sheets_ecids = parse_google_sheets_ecids(this_data)
    # mysql_ecids = parse_mysql_ecids(sheets_ecids)
    # compare_results = compare_ecid_results(sheets_ecids, mysql_ecids)
    # print compare_results

    # Get data from Google Sheet
    # Get completed rows and review rows from Google Sheet
    s0_this_ready_data = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                         this_sheet_title))[0]
    print "Completed Data Set"
    # print s0_this_ready_data, "\n"
    s0_this_ready_data_count = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                               this_sheet_title))[1]
    print "Completed Data Set Count"
    print s0_this_ready_data_count, "\n"
    s10this_not_ready_data = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                             this_sheet_title))[2]
    print "Review Data Set"
    # print s0_this_not_ready_data, "\n"
    s0_this_not_ready_data_count = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                                   this_sheet_title))[3]
    print "Review Data Set Count"
    print s0_this_not_ready_data_count, "\n"
    s0_this_invalid_data = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                           this_sheet_title))[4]
    print "Invalid Data Set"
    print s0_this_invalid_data, "\n"
    s0_this_invalid_data_count = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                                 this_sheet_title))[5]
    print "Invalid Data Set Count"
    print s0_this_invalid_data_count, "\n"
    data_set_type = "completed"
    update_mysql_complete(s0_this_ready_data, this_batch_id, data_set_type)
    data_set_type = "invalid"
    update_mysql_complete(s0_this_invalid_data, this_batch_id, data_set_type)
    # logging.info("\nUpdate MySQL database\n")
    # update_mysql_complete(s0_this_ready_data, this_batch_id)
    # update_mysql_source(s0_this_ready_data, this_batch_id)
    # # Verify Google Sheet data was loaded to MySQL
    # gs_count = calculate_google_sheets_data(s0_this_ready_data)
    # mysql_count = calculate_mysql_complete(this_batch_id)

    # review_type_num = 2
    # review_data_gs_count = calculate_google_sheets_data(get_google_sheets_data(this_spreadsheet_id, this_range_name))
    # review_this_s_start_columnn_index = 4
    # review_this_s_end_column_index = 6
    # review_this_p_start_row_index = 2
    # review_this_p_end_row_index = review_this_p_start_row_index + s0_this_not_ready_data_count
    # review_this_p_start_column_index = 4
    # review_this_p_end_column_index = 6
    # copy_paste_format(this_spreadsheet_id, review_type_num, this_sheet_title, review_this_s_start_columnn_index,
    #                   review_this_s_end_column_index, review_this_p_start_row_index, review_this_p_end_row_index,
    #                   review_this_p_start_column_index, review_this_p_end_column_index)

    # For debugging purposes for the parse and get functions above
    # logging.info("\nGoogle Sheet Data Set Ready\n")
    # logging.info("\nGoogle Sheet Data Set Not Ready\n")

    # Used for retrieving data from MySQL for data prep for production run
    # calculate_mysql_naics_counts('541511')
    # calculate_mysql_naics_counts('511210')

if ready_state == 1:
    logging.info("Ready State %s", ready_state)
    delete_backup_sheet(this_spreadsheet_id)
    backup_sheet_id = get_google_sheet_properties(this_spreadsheet_id, this_sheet_title)[0]
    create_backup_sheet(this_spreadsheet_id, backup_sheet_id, this_batch_id)
    print("\nBackup Sheet Created")
    # Get data from Google Sheet
    # Get completed rows and review rows from Google Sheet
    s1_this_ready_data = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                         this_sheet_title))[0]
    print "Completed Data Set"
    # print s1_this_ready_data, "\n"
    s1_this_ready_data_count = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                               this_sheet_title))[1]
    print "Completed Data Set Count"
    print s1_this_ready_data_count, "\n"
    s1_this_not_ready_data = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                             this_sheet_title))[2]
    print "Review Data Set"
    # print s1_this_not_ready_data, "\n"
    s1_this_not_ready_data_count = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                                   this_sheet_title))[3]
    print "Review Data Set Count"
    print s1_this_not_ready_data_count, "\n"
    s1_this_invalid_data = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                           this_sheet_title))[4]
    print "Invalid Data Set"
    print s1_this_invalid_data, "\n"
    s1_this_invalid_data_count = parse_google_sheets_data(get_google_sheets_data(this_spreadsheet_id,
                                                                                 this_sheet_title))[5]
    print "Invalid Data Set Count"
    print s1_this_invalid_data_count, "\n"

    if s1_this_ready_data_count > 2:
        print("\nGoogle Sheet Data Set Completed\n")
        data_set_type = "completed"
        update_mysql_complete(s1_this_ready_data, this_batch_id, data_set_type)
        data_set_type = "invalid"
        update_mysql_complete(s1_this_invalid_data, this_batch_id, data_set_type)
        update_mysql_source(s1_this_ready_data, this_batch_id)
    else:
        print("\nNo data found for Completed records. Set ready_state = 2 to clear and reload Google Sheet.")

    # Verify Google Sheet data was loaded to MySQL
    gs_count_complete = calculate_google_sheets_data(s1_this_ready_data)
    mysql_count_complete = calculate_mysql_complete(this_batch_id)
    # Compare data set counts in Google Sheets with MySQL for Completed Data
    this_data = get_google_sheets_data(this_spreadsheet_id, this_range_name)
    sheets_ecids = parse_google_sheets_ecids(this_data)
    mysql_ecids = parse_mysql_ecids(sheets_ecids)
    compare_results = compare_ecid_results(sheets_ecids, mysql_ecids)
    print compare_results
    if compare_results:
        print("\nData was successfully retrieved from Google Sheets and loaded into MySQL")
        print("\nGoogle Sheets row count = %s", str(gs_count_complete))
        print("\nMySQL row count = %s", str(mysql_count_complete))
        print("\nClear current data set from Google Sheet")
        clear_google_sheets_data(this_spreadsheet_id, this_range_name)
        # Load Review Data set into Google Sheets
        if s1_this_not_ready_data_count > 2:
            print("\nGoogle Sheet Data Set Review\n")
            review_data_start_row = update_google_sheets_data(this_spreadsheet_id, this_range_name,
                                                              s1_this_not_ready_data)[0]
            review_data_range = update_google_sheets_data(this_spreadsheet_id, this_range_name,
                                                          s1_this_not_ready_data)[1]
            review_data_gs_count = calculate_google_sheets_data(
                get_google_sheets_data(this_spreadsheet_id, this_range_name))
            # Copy Google Sheets formulas for all columns for Review Data Set
            review_type_num = 2
            review_this_s_start_columnn_index = 4
            review_this_s_end_column_index = 6
            review_this_p_start_row_index = 2
            review_this_p_end_row_index = review_this_p_start_row_index + s1_this_not_ready_data_count
            review_this_p_start_column_index = 4
            review_this_p_end_column_index = 6
            copy_paste_format(this_spreadsheet_id, review_type_num, this_sheet_title, review_this_s_start_columnn_index,
                              review_this_s_end_column_index, review_this_p_start_row_index,
                              review_this_p_end_row_index,
                              review_this_p_start_column_index, review_this_p_end_column_index)

            print("\nUpdate Google Sheet with New MySQL Data")
            this_new_data, this_new_data_count = get_mysql_source(this_naics_code, this_limit)
            new_data_range = review_data_range + ":D" + str(this_new_data_count + review_data_start_row)
            update_google_sheets_data(this_spreadsheet_id, new_data_range, this_new_data)
            # append_google_sheets_data(this_sheet_id, this_range_name, this_data_values)
            new_data_gs_count = calculate_google_sheets_data(
                get_google_sheets_data(this_spreadsheet_id, this_range_name))

            this_start_row_index = this_sheet_header + review_data_gs_count
            this_end_row_index = review_data_gs_count + new_data_gs_count
            # Copy/Paste formulas for range E:Q
            new_type_num = 1
            new_this_s_start_row_index = 4
            new_this_s_end_column_index = 17
            new_this_p_start_row_index = review_this_p_end_row_index
            # Count of Google Sheet minus the rows of data in the Not Ready Data plus two header rows
            new_this_p_end_row_index = new_data_gs_count - review_data_gs_count + this_sheet_header
            print "\n" + str(new_this_p_end_row_index) + "\n"
            new_this_p_start_column_index = 4
            new_this_p_end_column_index = 17
            copy_paste_format(this_spreadsheet_id, new_type_num, this_sheet_title, new_this_s_start_row_index,
                              new_this_s_end_column_index,
                              new_this_p_start_row_index, new_this_p_end_row_index, new_this_p_start_column_index,
                              new_this_p_end_column_index)

            # Format columns for range A:D
            column_data_format(this_spreadsheet_id, this_sheet_title, this_end_row_index)
        else:
            print("\nNo data found for Review records.")
            print("\nUpdate Google Sheet with New MySQL Data")
            this_new_data, this_new_data_count = get_mysql_source(this_naics_code, this_limit)
            new_data_range = this_sheet_title + "!A3:D" + str(this_new_data_count + 2)
            update_google_sheets_data(this_spreadsheet_id, new_data_range, this_new_data)
            # append_google_sheets_data(this_sheet_id, this_range_name, this_data_values)
            new_data_gs_count = calculate_google_sheets_data(
                get_google_sheets_data(this_spreadsheet_id, this_range_name))
            # Copy/Paste formulas for range E:Q
            new_type_num = 1
            new_this_s_start_row_index = 4
            new_this_s_end_column_index = 17
            new_this_p_start_row_index = 2
            # Count of Google Sheet minus the rows of data in the Not Ready Data plus two header rows
            new_this_p_end_row_index = new_data_gs_count + this_sheet_header
            print "\n" + str(new_this_p_end_row_index) + "\n"
            new_this_p_start_column_index = 4
            new_this_p_end_column_index = 17
            copy_paste_format(this_spreadsheet_id, new_type_num, this_sheet_title, new_this_s_start_row_index,
                              new_this_s_end_column_index,
                              new_this_p_start_row_index, new_this_p_end_row_index, new_this_p_start_column_index,
                              new_this_p_end_column_index)
            # Format columns for range A:D
            this_end_row_index = new_data_gs_count
            column_data_format(this_spreadsheet_id, this_sheet_title, this_end_row_index)
    else:
        print "\nGoogle Sheets row count did not match MySQL row count"
        print("Google Sheets row count =  %s", str(gs_count_complete)), "\n"
        print("\nMySQL row count =  %s", str(mysql_count_complete))
        print("\nCheck for data in MySQL for Batch ID:  %s", str(this_batch_id))

if ready_state == 2:
    logging.info("Ready State %s", ready_state)
    clear_google_sheets_data(this_spreadsheet_id, this_range_name)
    print "\nUpdate Google Sheet with New MySQL Data"
    this_new_data = get_mysql_source(this_naics_code, this_limit)[0]
    this_new_data_count = get_mysql_source(this_naics_code, this_limit)[1]
    new_data_range = this_sheet_title + "!A3:D" + str(this_new_data_count + 2)
    update_google_sheets_data(this_spreadsheet_id, new_data_range, this_new_data)
    # Format columns for range A:D
    column_data_format(this_spreadsheet_id, this_sheet_title, this_new_data_count)
    # Copy/Paste formulas for range E:Q
    new_type_num = 1
    new_this_s_start_row_index = 4
    new_this_s_end_column_index = 17
    new_this_p_start_row_index = 2
    new_this_p_end_row_index = this_new_data_count
    new_this_p_start_column_index = 4
    new_this_p_end_column_index = 17
    copy_paste_format(this_spreadsheet_id, new_type_num, this_sheet_title, new_this_s_start_row_index,
                      new_this_s_end_column_index,
                      new_this_p_start_row_index, new_this_p_end_row_index, new_this_p_start_column_index,
                      new_this_p_end_column_index)

logging.info("Job took %s.", datetime.now() - start_time)

print "Script Complete"
