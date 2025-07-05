import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']
file_path = '/home/project/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names= attribute_list)
df.to_sql(table_name, conn, if_exists= 'replace', index=False)

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
#  1.In the same database STAFF, create another table called Departments. The attributes of the table are as shown below.

# Header	Description
# DEPT_ID	Department ID
# DEP_NAME	Department Name
# MANAGER_ID	Manager ID
# LOC_ID	Location ID
table2 = 'Departments'
attr = ['DEPT_ID','DEP_NAME','MANAGER_ID','LOC_ID']
# 2.Populate the Departments table with the data available in the CSV file which can be downloaded from the link below using wget
# https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/Departments.csv
file = '/home/project/Departments.csv'
df2 = pd.read_csv(file, names= attr)
df2.to_sql(table2,conn,if_exists='replace',index= False)
# 3.Append the Departments table with the following information.
# Attribute	Value
# DEPT_ID	9
# DEP_NAME	Quality Assurance
# MANAGER_ID	30010
# LOC_ID	L0010
data2 = {'DEPT_ID':     [9],
         'DEP_NAME':    ['Quality Assurance'],
         'MANAGER_ID':  [30010],
         'LOC_ID':      ['L0010']
         }
# 4.Run the following queries on the Departments Table:
# a. View all entries
# b. View only the department names
# c. Count the total entries


data2_append = pd.DataFrame(data2)
data2_append.to_sql(table2,conn,if_exists='append',index= False)
query_statement = f"SELECT * FROM {table2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
query_statement = f"SELECT DEP_NAME FROM {table2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
query_statement = f"SELECT COUNT(*) FROM {table2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
conn.close()