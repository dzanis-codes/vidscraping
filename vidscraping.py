import requests
import bs4
from bs4 import BeautifulSoup
import sqlite3
import time
import csv
import re
import sys
import traceback

#create database connection and result table


path = '/LBData/VID_STR/vid_str.db'
conn = sqlite3.connect(path)


c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS results
           (name_id INTEGER PRIMARY KEY, uznemums, name, str_reg_nr, address, active_flag, registered_date, closed_date, status, timestamp)''')
# Save (commit) the changes
conn.commit()

name = 'na'
str_reg_nr = 'na'
address = 'na'
active_flag = 'na'
registered_date = 'na'
closed_date = 'na'


def savaksana(reg_nr):
    url = 'https://www6.vid.gov.lv/STRV/Data'

    files = {
        'IsPhysicalPerson': (None, 'false'),
        'IsLegalPerson': (None, 'true'),
        'Name': (None, None),
        'Surname': (None, None),
        'Code': (None, reg_nr),
        'search': (None, 'AtlasÄ«t'),
        'submit': (None, 'yes')
    }
    headers = {}

    r = requests.post(url, data = files, headers=headers)
    soup = BeautifulSoup(r.text, features="html.parser")
    #print(soup)

    k = soup.find(text=re.compile("darbÄ«bas apturÄ“Åanu"))
    print(k)
    if k != None:
        #Åeit vajag divus apakÅscenÄrijus. attiecÄ«gi - vai nu ir tÄ papildus tabula ar struktÅ«rvienÄ«bÄm vai nav vispÄr.
        check2 = soup.find(text=re.compile("IzslÄ“gts no NodokÄ¼u maksÄtÄju"))
        print("vai atrada vai ir izslÄ“gts:")
        print(check2)

        if check2 == None:
            #bÅ«tu labi ja Åeit tiktu pievienota informÄcija, ka Åim ir apturÄ“ta saimn.darbÄ«ba, bet tas ir optional

            try:
                registresana(soup, reg_nr)
            except Exception as e:

                print(e)
 
                uznemums = reg_nr

                ts = time.gmtime()
                timestamp = (time.strftime("%Y-%m-%d %H:%M:%S", ts))
                company_closed = 'Nav struktÅ«rvienÄ«bu'
                sql_entry = (uznemums, name, str_reg_nr, address, active_flag, registered_date, closed_date, company_closed, timestamp)
                enter_db(sql_entry)
            
        else:
            #pieregistre to ka uznemums beidzies un nevajag sturkturvienibu
            uznemums = reg_nr

            ts = time.gmtime()
            timestamp = (time.strftime("%Y-%m-%d %H:%M:%S", ts))
            company_closed = 'IzslÄ“gts no UR'
            sql_entry = (uznemums, name, str_reg_nr, address, active_flag, registered_date, closed_date, company_closed, timestamp)
            enter_db(sql_entry)
    else:

        info = soup.find("h2", {"class": "SDVHeader"})
        info_text = info.text
        check_if_closed = info_text.find('IzslÄ“gts')
        check_if_works = info_text.find('nav apturÄ“ta')
        print(check_if_closed)
        print(check_if_works)
        if check_if_closed > 0:
            print("closed")
            uznemums = reg_nr

            ts = time.gmtime()
            timestamp = (time.strftime("%Y-%m-%d %H:%M:%S", ts))
            company_closed = 'izslÄ“gts no UR'
            sql_entry = (uznemums, name, str_reg_nr, address, active_flag, registered_date, closed_date, company_closed, timestamp)
            enter_db(sql_entry)
        else:
            try:
                registresana(soup, reg_nr)
            except Exception as e:
            #sql_entry = (a, b, c) need legal address?
                print(e)
                print(check_if_works)
                uznemums = reg_nr

                ts = time.gmtime()
                timestamp = (time.strftime("%Y-%m-%d %H:%M:%S", ts))
                company_closed = 'Nav struktÅ«rvienÄ«bu'
                sql_entry = (uznemums, name, str_reg_nr, address, active_flag, registered_date, closed_date, company_closed, timestamp)
                enter_db(sql_entry)
            




def registresana(soup, reg_nr):

    find_header = soup.find_all('h2', text=re.compile('InformÄcija par nodokÄ¼u maksÄtÄja struktÅ«rvienÄ«bÄm'))[0]
    print(find_header)
    correct_table = find_header.find_next_sibling()
    #print(correct_table)




    table = correct_table

    my_table = table.find('tbody')
    #print(my_table)

    rows = my_table.find_all('tr')
    #print(rows)

    for row in rows:
        row_contents = []
        cells = row.find_all('td')
        for cell in cells:
            value = cell.string
            #print(value)

            v_list = value.split()
            clean_value = " ".join(v_list) 
            row_contents.append(clean_value)
        #print(row_contents) #debug mode :)
        name = row_contents[0]
        str_reg_nr = row_contents[1]
        address = row_contents[2]
        active_flag = row_contents[3]
        registered_date = row_contents[4]
        closed_date = row_contents[5]
        uznemums = reg_nr
        company_closed = 'nav izslÄ“gts no UR'
        ts = time.gmtime()
        timestamp = (time.strftime("%Y-%m-%d %H:%M:%S", ts))
        sql_entry = (uznemums, name, str_reg_nr, address, active_flag, registered_date, closed_date, company_closed, timestamp)
        print(sql_entry)
        enter_db(sql_entry)

def enter_db(sql_entry):
    #sql entry should consist of 10 items
    c.execute("INSERT INTO results VALUES (null, ?, ?, ?, ?, ?, ?, ?, ?, ?)", sql_entry)
    conn.commit()
    

#savaksana('40103504912', 'test')

count = 1
while count < 190000:
    try:
        with open('list.txt') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')

            for row in csv_reader:
                print(row)
                reg_nr = row[0]
                savaksana(reg_nr)
                time.sleep(3)
                count += 1
    except Exception as e:
        error_path = '/LBApp_log/vidscraping.txt'
        f = open(error_path, 'a+')
        f.write('\n %s \n' % e)
        tb = traceback.TracebackException.from_exception(e)
        f.write('\n'.join(tb.stack.format()))            
        f.close()
        time.sleep(10)
        pass     

sys.exit()
