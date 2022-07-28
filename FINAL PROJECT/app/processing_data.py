import glob
import time
from cassandra.cluster import Cluster
# All files and directories ending with .txt and that don't begin with a dot:
print(glob.glob("C:\kafka-demo\output\*.csv")) 
# doc tat ca cac data cu
list_old = glob.glob("C:\kafka-demo\output\*.csv")

cluster = Cluster()

session = cluster.connect('k1')

columns = ['ID' ,'QUARTER' ,'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 
            'OP_UNIQUE_CARRIER',
            'ORIGIN',
            'DEST','DISTANCE',
            'CRS_DEP_TIME', 
            'LABEL','prediction']

dataframes_list_old = []
dataframes_list_total_old = [] 
import pandas as pd
for i in range(len(list_old)):
    dataframes_list_old = pd.read_csv(list_old[i], names = columns)
    dataframes_list_total_old.append(dataframes_list_old)
    
df = pd.concat(dataframes_list_total_old).reset_index(drop=True)



for i in range(len(df)):
    session.execute("Insert into stream_data (id,quarter,month,day_of_month, day_of_week, \
                op_unique_carrier,origin, dest,distance, crs_dep_time, \
                label) values ("+"'"+str(df.iloc[:,0][i])+"'"+","+str(df.iloc[:,1][i])+"," 
                +str(df.iloc[:,2][i])+","+str(df.iloc[:,3][i])+","+str(df.iloc[:,4][i])+","+"'"+ str(df.iloc[:,5][i])+"'"+
                    ","+"'"+str(df.iloc[:,6][i])+"'"+","+"'"+str(df.iloc[:,7][i])+"'"+","+ 
                        str(df.iloc[:,8][i])+","+str(df.iloc[:,9][i])+","+str(df.iloc[:,10][i])+ ")")

while True:
# doc tat ca cac data moi
    time.sleep(15)
    list_new = glob.glob("C:\kafka-demo\output\*.csv")
    res = set(list_new) - set(list_old)
    if (res != set()): 
        columns = ['ID' ,'QUARTER' ,'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 
                    'OP_UNIQUE_CARRIER', 
                    'ORIGIN',
                    'DEST','DISTANCE',
                    'CRS_DEP_TIME', 
                    'LABEL','prediction']

        dataframes_list = []
        dataframes_list_total = [] 
        import pandas as pd
        for i in range(len(res)):
            dataframes_list = pd.read_csv(list(res)[i], names = columns)
            dataframes_list_total.append(dataframes_list)
            
        df_stream = pd.concat(dataframes_list_total).reset_index(drop=True)
        
# chua sua phan trong ngoac
        for i in range(len(df_stream)):
            session.execute("Insert into stream_data (id,quarter,month,day_of_month, day_of_week, \
                op_unique_carrier,origin, dest,distance, crs_dep_time, \
                label) values ("+"'"+str(df.iloc[:,0][i])+"'"+","+str(df.iloc[:,1][i])+"," 
                +str(df.iloc[:,2][i])+","+str(df.iloc[:,3][i])+","+str(df.iloc[:,4][i])+","+"'"+ str(df.iloc[:,5][i])+"'"+
                    ","+"'"+str(df.iloc[:,6][i])+"'"+","+"'"+str(df.iloc[:,7][i])+"'"+","+ 
                        str(df.iloc[:,8][i])+","+str(df.iloc[:,9][i])+","+str(df.iloc[:,10][i])+ ")")
        df_stream.to_csv("C:\kafka-demo\data_final\display.csv", index = False)
        list_old = list_new
    # for i in range(2):
    #     import subprocess

    #     spark_submit_str= "spark-submit test.py"
    #     process=subprocess.Popen(spark_submit_str,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True, shell=True)
    #     stdout,stderr = process.communicate()
    #     if process.returncode !=0:
    #         print(stderr)
    #     print(stdout)