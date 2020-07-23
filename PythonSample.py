import os
from ringcentral import SDK
import logging
import pyodbc
import json
import sys
from datetime import datetime, timedelta

currentDate = datetime.now()
dateFrom = datetime(year=2020,month=2,day=currentDate.day,hour=0,minute=0,microsecond=0)
dateTo =datetime(year=2020,month=2,day=currentDate.day+1,hour=0,minute=0,microsecond=0)

logfilename = datetime.now().strftime(r'\\myserver\RingCentral\Log\RingCentral_current_day_Log_%d_%m_%Y.log')
print(logfilename)
logging.basicConfig(filename=logfilename,format='%(levelname)s:%(message)s',level=logging.DEBUG)
#logging.FileHandler(logfilename,mode=a,encoding=None,delay=False)
logging.info('************************************** Starting RingCentral Call Log API **************************************************')
logging.info('Start Datetime : %s',dateFrom.strftime("%Y-%m-%dT%H:%M:00.000Z"))
logging.info('End Datetime : %s ' , dateTo.strftime("%Y-%m-%dT%H:%M:00.000Z"))

# PATH PARAMETERS
accountId = '+18131234567'
extensionId = '100'

#client parameters
RC_CLIENTID = 'ALdIwlosPOs7wl-2w88lKL'
RC_CLIENTSECRET = '8dsOWqqalU5KRdsEQdwRL_gHeOWUerNiTF93F4SrBKpXeQ'
RC_SERVER = 'https://platform.ringcentral.com'
RC_USERNAME = '+18131234567'
RC_PASSWORD = 'Test123()'
RC_EXTENSION = '111'

#SQL Server Settings
server = 'tcp:mysql' 
database = 'MYCRM' 
username = 'myuser' 
password = 'mypwd@4546' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# OPTIONAL QUERY PARAMETERS
queryParams = {
    'direction': [ 'Inbound', 'Outbound' ],
    'type': [ 'Voice' ],
    'view': 'Simple',
    'dateFrom': dateFrom.strftime("%Y-%m-%dT%H:%M:00.000Z"),
    'dateTo': dateTo.strftime("%Y-%m-%dT%H:%M:00.000Z"),
    'perPage': 1000
}


#Get call log for the last 15 minutes from Ring Central
rcsdk = SDK(RC_CLIENTID, RC_CLIENTSECRET, RC_SERVER)
platform = rcsdk.platform()
platform.login(RC_USERNAME, RC_EXTENSION,RC_PASSWORD)
responselog = platform.get(f'/restapi/v1.0/account/~/call-log', queryParams)
logging.info('Retrieving call log from RingCentral')

# Add the initial log record to SQL table
logParams = [dateFrom.strftime("%Y-%m-%dT%H:%M:00.000Z"),dateTo.strftime("%Y-%m-%dT%H:%M:00.000Z"),'Process Starting',responselog.text()]
qryLogStart = "EXEC [dbo].[usp_Process_RingCentral_CallLog_Master] ?,?,?,?"
cursor.execute(qryLogStart,logParams)
cnxn.commit()

# Write to .CSV
logging.info(r'Saving call log to local JSON file. Path: \\myserver\RingCentral\Response\rc_call_current_day_log_simple.json')
file_log = open(r'\\myserver\RingCentral\Response\rc_call_current_day_log_simple.json', "w")
file_log.write(responselog.text())
file_log.close()

# Loop thru call log file and save rows to SQL Server
logging.info('Looping thru call log file and saving rows to SQL Server')
recordCnt = 1

with open(r'\\myserver\RingCentral\Response\rc_call_current_day_log_simple.json') as json_file:
    data = json.load(json_file)
    for p in data['records']:
        try:
            callid = p['id']
            qrySPCallLogParams = repr(p['id'])
            qrySPCallLogParams += "," + repr(p['startTime']) 
            qrySPCallLogParams += "," + str(p['duration'])
            qrySPCallLogParams += "," + repr(p['direction'])
            qrySPCallLogParams += "," + repr(p['result'])
            qrySPCallLogParams += "," + repr(str(p['to'].get("phoneNumber","unknown")))
            qrySPCallLogParams += "," + repr(str(p['to'].get("extensionNumber","unknown")))
            qrySPCallLogParams += "," + repr(p['to'].get("location","unknown"))
            qrySPCallLogParams += "," + repr(p['from'].get("name","unknown")) 
            qrySPCallLogParams += "," + repr(str(p['from'].get("phoneNumber","unknown")))
            if repr(str(p['from'].get("extensionNumber","unknown"))) == "unknown":
                qrySPCallLogParams += "," + repr(str(p['from'].get("extensionId","unknown")))
            else:
                qrySPCallLogParams += "," + repr(str(p['from'].get("extensionNumber","unknown")))
            qrySPCallLogParams += "," + repr(p['telephonySessionId'])
            qrySPCallLogParams += "," + repr(p['sessionId'])
            qrySPCallLog = "EXEC [dbo].[usp_Process_RingCentral_CallLog] " + qrySPCallLogParams
            #print(qrySPCallLog)
            cursor.execute(qrySPCallLog)
            cnxn.commit()

            recordCnt += 1
        except:
            print('Errors: {}. {}, line: {}, Call ID: {}'.format(sys.exc_info()[0],
                                         sys.exc_info()[1],
                                         sys.exc_info()[2].tb_lineno,
                                         callid))

            logging.error('Errors: {}. {}, line: {}, Call ID: {}'.format(sys.exc_info()[0],
                                         sys.exc_info()[1],
                                         sys.exc_info()[2].tb_lineno,
                                         callid))

#End saving to sql
processResults = 'Process Completed - ' + str(recordCnt) + ' record(s)'
print(processResults)
logging.info(processResults)

# Add the end log record to SQL table
logEndParams = [dateFrom.strftime("%Y-%m-%dT%H:%M:00.000Z"),dateTo.strftime("%Y-%m-%dT%H:%M:00.000Z"),processResults,"NA"]
qryLogEnd = "EXEC [dbo].[usp_Process_RingCentral_CallLog_Master] ?,?,?,?"
cursor.execute(qryLogEnd,logEndParams)
cnxn.commit()