import bcp
import datetime
from config import server_info, database_testing, sftp_pwd_kiteworks
import warnings
import xlwt
import time
import paramiko
from pandas import ExcelWriter
import xlsxwriter

import time
import pandas as pd

from pathlib import Path

# Disable the warnings
warnings.filterwarnings('ignore')


# filename = f'dump_{time}'

# connect to the database TDAP
def download_clarity(path_raw_files,filename, database):

    conn = bcp.Connection(host= server_info, driver='mssql')
    my_bcp = bcp.BCP(conn)

    path = Path(f'{path_raw_files}\\{filename}.csv')
    file = bcp.DataFile(file_path= path, delimiter='|')
    my_bcp.dump(query= f'''SELECT loc_name
                                ,facility_id
                                ,department_name
                                ,hospital_service
                                ,order_time
                                ,order_procedure_id
                                ,specimen_type
                                ,procedure_name
                                ,covid19_proc_type
                                ,result
                                ,site_type
                            FROM [T2DAP].[ven].[covid19_pcr_testing_all_care_settings]
                            WHERE CAST(order_time AS DATE) >= DATEADD(day,-7, CAST(GETDATE() AS Date))
                            AND CAST(order_time AS DATE) <= DATEADD(day,-1, CAST(GETDATE() AS Date))
                            order by convert(date, order_time) desc
                            ''', output_file=file)
                            # or WHERE CAST(order_time AS DATE) BETWEEN DATEADD(day,-7, cast(GETDATE() AS Date)) AND DATEADD(day,-1, cast(GETDATE() AS Date))

''' Manipulate extracted data get raw data and grouped extraction '''

def create_output_file(path_raw_files,filename):

    columns = ['loc_name', 'facility_id', 'department_name','hospital_service','order_time', 'order_procedure_id',
                'specimen_type', 'procedure_name','covid19_proc_type','result','site_type']

    path_filename = f'{path_raw_files}\\{filename}.csv'
    raw_df = pd.read_csv(path_filename, delimiter='|' ,
                            dtype={"loc_name": str, "admit_dep_name":str ,"department_id":object ,"hospital_service":str},
                            names = columns,
                            parse_dates=['order_time'])
    new_df =raw_df.replace('|', ' ')

    # new_df.to_excel(f'{path_raw_files}{filename}.xlsx', index = None, sheet_name='Weekly_raw_data')

    # df = pd.read_excel(f'{path_raw_files}{filename}.xlsx',  parse_dates=['order_time'])

    # selecting rows based on condition
    department_needed = ['BE TIMES SQUARE TC','CI BENSONHURST 14 TC','CI FORT HAMILTON TC','CI GREENBELT REC TC','CI SORRENTINO REC TC','EL 51-30 NORTHERN BLVD TC','EY MIDWOOD PRE-K TC',
    'EY ST GEORGE FERRY TC','KC BAY RIDGE 5TH AVE TC','KC STARRETT CITY TC','LI ST JAMES REC CTR TC','NO CO-OP CITY RET TC','NO COOP CITY RE TC','NO RAIN BOSTON RD TC','NO RAIN BSTN RD TC']

    rslt_df = new_df[new_df['department_name'].isin(department_needed)][['department_name', 'procedure_name','order_time']]
    print(rslt_df)
    # subset_df = df[(df['department_name'] == 'BE TIMES SQUARE TC') | (df['department_name'] == 'CI BENSONHURST 14 TC') |
    #                 (df['department_name'] == 'CI FORT HAMILTON TC') |
    #                 (df['department_name'] == 'CI GREENBELT REC TC') |
    #                  (df['department_name'] == 'CI SORRENTINO REC TC') |
    #                 (df['department_name'] == 'EL 51-30 NORTHERN BLVD TC')|
    #                 (df['department_name'] == 'EY MIDWOOD PRE-K TC') |
    #                 (df['department_name'] == ' EY ST GEORGE FERRY TC') |
    #                 (df['department_name'] == 'KC BAY RIDGE 5TH AVE TC')|
    #                 (df['department_name'] == 'KC STARRETT CITY TC')|
    #                 (df['department_name'] == 'LI ST JAMES REC CTR TC')|
    #                 (df['department_name'] == 'NO CO-OP CITY RET TC')|
    #                 (df['department_name'] == 'NO COOP CITY RE TC')|
    # #                 (df['department_name'] == 'NO RAIN BOSTON RD TC')|
    #                 (df['department_name'] == 'NO RAIN BSTN RD TC')]

    # subset_df = new_df.query('department_name in ["BE TIMES SQUARE TC","CI BENSONHURST 14 TC","CI FORT HAMILTON TC","CI GREENBELT REC TC","CI SORRENTINO REC TC","EL 51-30 NORTHERN BLVD TC","EY MIDWOOD PRE-K TC","EY ST GEORGE FERRY TC","KC BAY RIDGE 5TH AVE TC","KC STARRETT CITY TC","LI ST JAMES REC CTR TC","NO CO-OP CITY RET TC","NO COOP CITY RE TC","NO RAIN BOSTON RD TC","NO RAIN BSTN RD TC"]')

    replaced_df=rslt_df.replace({'ACCESSDX COVID-19 PCR':'NON-RAPID','BIOFIRE COVID-19 PCR':'NON-RAPID', 'BIOFIRE RESPIRATORY PANEL WITH COVID-19':'NON-RAPID','BIOREFERENCE COVID-19 PCR(SO)':'NON-RAPID','ACCESSDX COVID-19 PCR':'NON-RAPID','CEPHEID COVID-19 PCR':'NON-RAPID',
                        'CEPHEID COVID-19 RSV INFLUENZA A/B PCR':'NON-RAPID','COBAS COVID-19 INFLUENZA A/B PCR':'NON-RAPID', 'COBAS COVID-19 PCR':'NON-RAPID','DOHMH COVID-19 TEST-LOWER RESPIRATORY(SO)NYC PHL':'NON-RAPID','DOHMH COVID-19 TEST-NASOPHARYNGEAL(SO)NYC PHL':'NON-RAPID','DOHMH COVID-19 TEST-OROPHARYNGEAL(SO)NYC PHL':'NON-RAPID',
                        'GENMARK RESPIRATORY PANEL 2 PCR':'NON-RAPID','LABCORP COVID-19 TEST (SEND OUT)':'NON-RAPID', 'NW COVID-19 PCR(SEND OUT)':'NON-RAPID','PRL COVID19':'NON-RAPID','POC LIAT COVID-19 FLU A/B RT-PCR':'NON-RAPID','QUEST COVID-19 PCR (SO)':'NON-RAPID','RESPIRATORY VIRAL/BACTI DETECTION BY NAT (SEND OUT)':'NON-RAPID'})
    print(replaced_df.head())
    replaced_df.replace({"POC BINAX COVID-19 AG RAPID": "RAPID (POC BINAX COVID-19 AG RAPID and POC RAPID COVID-19)", "POC RAPID COVID-19": "RAPID (POC BINAX COVID-19 AG RAPID and POC RAPID COVID-19)"}, inplace=True)
    print(replaced_df.head())

    replaced_df['order_time']=replaced_df['order_time'].dt.strftime('%B %d, %Y')
    saved_df=replaced_df.groupby(['department_name', 'procedure_name', 'order_time'])['order_time'].agg('count')
    print(saved_df)
    final_df=saved_df.unstack()
    print(final_df)


    with pd.ExcelWriter(f'report_{today}.xlsx') as writer:
        new_df.to_excel(writer, engine='xlsxwriter', index = None, sheet_name='Weekly_raw_data')
        final_df.to_excel(writer, engine='xlsxwriter', sheet_name='Report_aggregated_data')

    # replaced_df.to_excel(f'report{today}.xlsx', index = None, header=True, sheet_name="Count_of_tests_by_locations")

#files is an array of the path to files we want to put to kiteworks
#target folder it name of folder in kiteworks we want to put files in

def upload_to_kiteworks(files, target_folder):
    #opens connection to kiteworks
    hostname, port = 'secure.nychhc.org', 22
    sftp_uid = 's-t2dapfiles'
    t = paramiko.Transport((hostname, port))
    t.connect(username=sftp_uid, password=f'{sftp_pwd_kiteworks}')
    sftp = paramiko.SFTPClient.from_transport(t) #back to pysftp wrapper


    for file in files:
        print(file)
        sftp.put(file, (target_folder +"/" + file))



#entrypoint
if __name__ == '__main__':


    path_raw_files = r'C:\Users\a-sartoria1\rnav_report_clarity\rnva_reports'
# path_raw_files=r'C:\Users\sartoria1\Desktop\prova\raw_files'
# print()
    # today= (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = f'reportrnva_week_{today}'
    start = time.time()
    download_clarity(path_raw_files=path_raw_files,filename=filename, database=database_testing)
    stop = time.time()
    print(stop - start)

    create_output_file(path_raw_files,filename)

    files =[f'report_{today}.xlsx']
    upload_to_kiteworks(files = files, target_folder="RNAV Engagement & Testing Report")
