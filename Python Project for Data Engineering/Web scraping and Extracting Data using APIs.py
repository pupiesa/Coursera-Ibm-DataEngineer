# Modify the code to extract Film, Year, and Rotten Tomatoes' Top 100 headers.

# Restrict the results to only the top 25 entries.

# Filter the output to print only the films released in the 2000s (year 2000 included). 
import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = '/home/project/top_50_films.csv'
df = pd.DataFrame(columns= ['Film','Year',"Rotten Tomatoes' Top 100"])
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')
print(rows)
for row in rows:
    if count < 26:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {"Film": col[1].contents[0],
                         "Year": col[2].contents[0],
                         "Rotten Tomatoes' Top 100": col[3].contents[0],}
            if int(data_dict['Year']) >= 2000 and int(data_dict['Year']) <= 2009:
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
            count+=1
    else:
        break
df.to_csv(csv_path)
conn = sqlite3.connect(db_name)
df.to_sql(table_name,conn,if_exists='replace',index=False)
conn.close()


