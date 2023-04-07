
import pymssql
import mysql.connector

# TEMS DB Info
temsDbHost = '****'
temsDbName = '*****'
temsDbUser = '*****'
temsDbPass ='******'

# enTIMS DB Info
enTIMSDbHost = '******'
enTIMSDbName = '*****'
enTIMSDbUser = '*****'
enTIMSDbPass = '*****'

# TEMS DB Connection
temsConnection = pymssql.connect(temsDbHost, temsDbUser, temsDbPass, temsDbName) 
temsCursor = temsConnection.cursor() 

# enTIMS DB Connection
enTIMSConnection = mysql.connector.connect(host=enTIMSDbHost, user=enTIMSDbUser, password=enTIMSDbPass, database=enTIMSDbName)
enTIMSCursor = enTIMSConnection.cursor()

# Insert Query for ewsn_data_chainages_unit_id
ewsnDataChainageSql = "INSERT INTO ewsn_data_chainages_unit_155 (chainage, loop_number, value, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)"

# Select DatCH TEMS all data
temsCursor.execute('Select * from DatCH;')  
dataChRow = temsCursor.fetchone()  

prepareDataCh = []
insertCount = 1
print("Start Inserting to ewsn_data_chainages_unit_155")
while dataChRow:  
    prepareDataCh.append((dataChRow[0], dataChRow[1], dataChRow[2], dataChRow[3], dataChRow[3])) 
    
    if len(prepareDataCh) > 1000 : 
        enTIMSCursor.executemany(ewsnDataChainageSql, prepareDataCh)
        enTIMSConnection.commit()
        prepareDataCh = []  
        
        if enTIMSCursor.rowcount > 0 :
            print(insertCount, "row was inserted!")      

    dataChRow = temsCursor.fetchone()  
    insertCount = insertCount + 1

# Execute Query for ewsn_data_chainages_unit_id
enTIMSCursor.executemany(ewsnDataChainageSql, prepareDataCh)
enTIMSConnection.commit()
print("Completed ",insertCount," rows inserted to ewsn_data_chainages_unit_156")

# Insert Query for ewsn_data_chainages_unit_id
ewsnDataRingDetailSql = "INSERT INTO ewsn_data_ring_details_unit_156 (ring_number, net_stroke, loop_number, value, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"

# Select DatRingDetail TEMS all data 
temsCursor.execute('Select * from DatRingDetail;')
dataRingDetailRow = temsCursor.fetchone()

prepareDataRingDetail = []
insertCount = 1
print("Start Inserting to ewsn_data_ring_details_unit_156")
while dataRingDetailRow:
    prepareDataRingDetail.append((dataRingDetailRow[0],dataRingDetailRow[2],dataRingDetailRow[1],dataRingDetailRow[3], dataRingDetailRow[4], dataRingDetailRow[4]))
    if len(prepareDataRingDetail) > 1000:
        enTIMSCursor.executemany(ewsnDataRingDetailSql, prepareDataRingDetail)
        enTIMSConnection.commit()
        prepareDataRingDetail = []
        if enTIMSCursor.rowcount > 0:
            print(insertCount, "Row was inserted.")
    dataRingDetailRow=temsCursor.fetchone()
    insertCount = insertCount + 1

# Execute Query for ewsn_data_chainages_unit_id
enTIMSCursor.executemany(ewsnDataRingDetailSql, prepareDataRingDetail)
enTIMSConnection.commit()
print("Completed ",insertCount," rows inserted to ewsn_data_ring_details_unit_156")

temsConnection.close()
enTIMSConnection.close()

