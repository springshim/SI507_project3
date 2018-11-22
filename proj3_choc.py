import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

statement = '''
    DROP TABLE IF EXISTS 'Bars';
'''
cur.execute(statement)

statement = '''
    DROP TABLE IF EXISTS 'Countries';
'''
cur.execute(statement)
conn.commit()


statement = '''
    CREATE TABLE 'Countries' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Alpha2' TEXT NOT NULL,
        'Alpha3' TEXT NOT NULL,
        'EnglishName' TEXT NOT NULL,
        'Region' TEXT NOT NULL,
        'Subregion' TEXT NOT NULL,
        'Population' Integer NOT NULL,
        'Area' Real
    );
'''
cur.execute(statement)


statement = '''
    CREATE TABLE 'Bars' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Company' TEXT NOT NULL,
        'SpecificBeanBarName' TEXT NOT NULL,
        'REF' TEXT NOT NULL,
        'ReviewDate' TEXT NOT NULL,
        'CocoaPercent' REAL,
        'CompanyLocation' TEXT,
        'CompanyLocationId' INTEGER,
        'Rating' REAL,
        'BeanType' TEXT NOT NULL,
        'BroadBeanOrigin' TEXT,
        'BroadBeanOriginId' INTEGER
    );
'''
cur.execute(statement)


f = open(COUNTRIESJSON, "r", encoding='utf8')
jsonFile = f.read()
res = json.loads(jsonFile)
f.close()

for row in res:
    insertion = (None, row['alpha2Code'], row['alpha3Code'], row['name'], row['region'], row['subregion'], row['population'], row['area'])
    statement = 'INSERT INTO "Countries" '
    statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    cur.execute(statement, insertion)
    conn.commit()



with open(BARSCSV, encoding='utf8') as csvFile:
    res = csv.reader(csvFile)

    for row in res:
        CocoaPercent = str(row[4].replace('%',' '))
        row[4] = CocoaPercent
        countryId = cur.execute('SELECT Id, EnglishName from Countries').fetchall()
        for index in countryId:
            if (row[5] == index[1]):
                CompanyLocationId = index[0]
            if (row[8] == index[1]):
                BroadBeanOriginId = index[0]

        if (row[0] != "Company"):
            insertion = (None, row[0], row[1], row[2], row[3], row[4], row[5], CompanyLocationId, row[6], row[7], row[8], BroadBeanOriginId)
            statement = 'INSERT INTO "Bars" '
            statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            cur.execute(statement, insertion)
            conn.commit()



# Part 2: Implement logic to process user commands
def process_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    parameter = command.split()

    results = []
    params = []

    if parameter[0] == 'bars':
        statement = 'SELECT "SpecificBeanBarName", "Company", "CompanyLocation", "Rating", "CocoaPercent", "BroadBeanOrigin" '
        statement += 'FROM "Bars" '

        if len(parameter) == 1:
            statement += 'ORDER BY "Rating" DESC '
            statement += 'LIMIT 10 '            

        else:
            if 'sellcountry' in parameter[1]:
                statement += 'JOIN "Countries" ON Countries.Alpha2 = ? '
                statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
                params.append(parameter[1][-2:])

            elif 'sourcecountry' in parameter[1]:
                statement += 'JOIN "Countries" ON Countries.Alpha2 = ? '
                statement += 'WHERE Bars.BroadBeanOrigin = Countries.EnglishName '
                params.append(parameter[1][-2:])
               
            elif 'sellregion' in parameter[1]:
                statement += 'JOIN "Countries" ON Countries.Region = ? '
                statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
                params.append(parameter[1][13:])

            elif 'sourceregion' in parameter[1]:
                statement += 'JOIN "Countries" ON Countries.Region = ? '
                statement += 'WHERE Bars.BroadBeanOrigin = Countries.EnglishName '
                params.append(parameter[1][13:])


            if 'ratings' in parameter:
                statement += 'ORDER BY "Rating" '

            elif 'cocoa' in parameter:
                statement += 'ORDER BY "CocoaPercent" '
                print("Cocoa")

            else:
                statement += 'ORDER BY "Rating" '


            if 'top' in command:
                statement += 'DESC '
                statement += 'LIMIT ? '
                params.append(parameter[-1][4:])

            elif 'bottom' in command:
                statement += 'LIMIT ? '
                params.append(parameter[-1][7:])

            else:
                statement += 'DESC '
                statement += 'LIMIT 10 '


        cur.execute(statement, params)
        results = cur.fetchall()
        for row in results:
            result = '{0:40} {1:32} {2:32} {3:6} {4:6} {5:16}'.format(row[0], row[1], row[2], row[3], row[4], row[5])
            print(result)



    elif parameter[0] == 'companies':
        if len(parameter) == 1:
            statement = 'SELECT "Company","CompanyLocation",AVG("Rating"),COUNT(*) '
            statement += 'FROM "Bars" '
            statement += 'GROUP BY "Company" '
            statement += 'HAVING COUNT(*) > 4 '
            statement += 'ORDER BY AVG("Rating") DESC '
            statement += 'LIMIT 10 ' 
            
        else:           
            if 'ratings' in parameter:
                statement = 'SELECT "Company", "CompanyLocation", AVG("Rating"), COUNT(*) '
                statement += 'FROM "Bars" '

            elif 'cocoa' in parameter:
                statement = 'SELECT "Company", "CompanyLocation", AVG("CocoaPercent"), COUNT(*) '
                statement += 'FROM "Bars" ' 

            elif 'bars_sold' in parameter:
                statement = 'SELECT "Company","CompanyLocation", COUNT(*) '
                statement += 'FROM "Bars" ' 

            else:
                statement = 'SELECT "Company", "CompanyLocation", AVG("Rating"), COUNT(*) '
                statement += 'FROM "Bars" '


            if 'country' in parameter[1]:
                statement += 'JOIN "Countries" ON Countries.Alpha2 = ? '
                statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
                params.append(parameter[1][-2:])

            elif 'region' in parameter[1]:
                statement += 'JOIN "Countries" ON Countries.Region = ? '
                statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
                params.append(parameter[1][7:])

            statement += 'GROUP BY "Company" '
            statement += 'HAVING COUNT(*) > 4 '

            if 'ratings' in parameter:
                statement += 'ORDER BY AVG(Rating) '

            elif 'cocoa' in parameter:                      
                statement += 'ORDER BY AVG(CocoaPercent) '

            elif 'bars_sold' in parameter:              
                statement += 'ORDER BY COUNT(*) '

            else:
                statement += 'ORDER BY AVG(Rating) '

            if 'top' in command:
                statement += 'DESC '
                statement += 'LIMIT ? '
                params.append(parameter[-1][4:])

            elif 'bottom' in command:
                statement += 'LIMIT ? '
                params.append(parameter[-1][7:])

            else:
                statement += 'DESC '
                statement += 'LIMIT 10 '

        cur.execute(statement, params)
        results = cur.fetchall()
        for row in results:
            result = '{0:40} {1:32} {2:3.2f}'.format(row[0], row[1], row[2])
            print(result)



    elif parameter[0] == 'countries':
        if len(parameter) == 1:
            statement = 'SELECT "CompanyLocation", "Region", AVG(Rating),COUNT(*) '
            statement += 'FROM "Bars" '
            statement += 'JOIN "Countries" '
            statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
            statement += 'GROUP BY "CompanyLocation" '   
            statement += 'HAVING COUNT(*) > 4 '
            statement += 'ORDER BY AVG("Rating") DESC '
            statement += 'LIMIT 10 ' 

        else:
            if 'sellers' in parameter:
                statement = 'SELECT "CompanyLocation", "Region", '

            elif 'sources' in parameter:
                statement = 'SELECT "BroadBeanOrigin", "Region", '

            else:
                statement = 'SELECT "CompanyLocation", "Region", '


            if 'ratings' in parameter:
                statement += 'AVG(Rating),COUNT(*) '
                statement += 'FROM "Bars" '
            
            elif 'cocoa' in parameter:
                statement += 'AVG(CocoaPercent),COUNT(*) '
                statement += 'FROM "Bars" '
            
            elif 'bars_sold' in parameter:
                statement += 'COUNT(*) '
                statement += 'FROM "Bars" '

            else:
                statement += 'AVG(Rating),COUNT(*) '
                statement += 'FROM "Bars" '


            if 'region' in parameter[1]:
                statement += 'JOIN "Countries" ON Countries.Region = ? '
                params.append(parameter[1][7:])

            else:
                statement += 'JOIN "Countries" '


            if 'sellers' in parameter:
                statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
                statement += 'GROUP BY "CompanyLocation" '

            elif 'sources' in parameter:
                statement += 'WHERE Bars.BroadBeanOrigin = Countries.EnglishName '
                statement += 'GROUP BY "BroadBeanOrigin" '

            else:
                statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
                statement += 'GROUP BY "CompanyLocation" '


            statement += 'HAVING COUNT(*) > 4 '


            if 'ratings'in parameter:
                statement += 'ORDER BY AVG(Rating) '

            elif 'cocoa'in parameter:
                statement += 'ORDER BY AVG(CocoaPercent) '

            elif 'bars_sold'in parameter:
                statement += 'ORDER BY COUNT(*) '

            else:
                statement += 'ORDER BY AVG(Rating) '


            if 'top' in command:
                statement += 'DESC '
                statement += 'LIMIT ? '
                params.append(parameter[-1][4:])

            elif 'bottom' in command:
                statement += 'LIMIT ? '
                params.append(parameter[-1][7:])

            else:
                statement += 'DESC '
                statement += 'LIMIT 10 '


        cur.execute(statement, params)
        results = cur.fetchall()
        for row in results:
            result = '{0:40} {1:32} {2:3.2f}'.format(row[0], row[1], row[2])
            print(result)


    elif parameter[0] == 'regions':
        statement = 'SELECT "Region", '

        if len(parameter) == 1:
            statement += 'AVG(Rating),COUNT(*) '
            statement += 'FROM "Bars" '   
            statement += 'JOIN "Countries" ON Countries.Region <> "Unknown" '
            statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
            statement += 'GROUP BY "Region" '
            statement += 'HAVING COUNT(*) > 4 '
            statement += 'ORDER BY AVG(Rating) '
            statement += 'DESC '
            statement += 'LIMIT 10 '           

        else:
            if 'ratings' in parameter:
                statement += 'AVG(Rating),COUNT(*) '
                statement += 'FROM "Bars" '

            elif 'cocoa' in parameter:
                statement += 'AVG(CocoaPercent),COUNT(*) '
                statement += 'FROM "Bars" '

            elif 'bars_sold' in parameter:
                statement += 'COUNT(*) '
                statement += 'FROM "Bars" '

            else:
                statement += 'AVG(Rating),COUNT(*) '
                statement += 'FROM "Bars" '

            statement += 'JOIN "Countries" ON Countries.Region <> "Unknown" '

            if 'sellers' in parameter:
                statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '

            elif 'sources' in parameter:
                statement += 'WHERE Bars.BroadBeanOrigin = Countries.EnglishName '

            else:
                statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '

            statement += 'GROUP BY "Region" '
            statement += 'HAVING COUNT(*) > 4 '

            if 'ratings'in parameter:
                statement += 'ORDER BY AVG(Rating) '

            elif 'cocoa'in parameter:
                statement += 'ORDER BY AVG(CocoaPercent) '

            elif 'bars_sold'in parameter:
                statement += 'ORDER BY COUNT(*) '

            else:
                statement += 'ORDER BY AVG(Rating) '


            if 'top' in command:
                statement += 'DESC '
                statement += 'LIMIT ? '
                params.append(parameter[-1][4:])

            elif 'bottom' in command:
                statement += 'LIMIT ? '
                params.append(parameter[-1][7:])

            else:
                statement += 'DESC '
                statement += 'LIMIT 10 '


        cur.execute(statement, params)
        results = cur.fetchall()
        for row in results:
            result = '{0:40} {1:3.2f}'.format(row[0], row[1])
            print(result)

    return results

# process_command('bars ratings')


def load_help_text():
    with open('help.txt') as f:
        return f.read()


# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        command = response.split()

        if response == 'help':
            print(help_text)
            continue


        if len(command) == 1:
            if command[0] in ['bars', 'companies', 'countries', 'regions', 'help', 'exit']:
                process_command(response)
                print("\n")  

            else:
                print('Command not recognized: ' + response)             

        elif len(command) == 0:
            print("You didn't type anything")

        else:
            if command[0] == 'bars':
                if (command[1].startswith ('sellcountry')) or (command[1].startswith ('sourcecountry')) or (command[1].startswith ('sellregion')) or (command[1].startswith ('sourceregion')) or (command[1].startswith ('ratings')) or (command[1].startswith ('cocoa')) or (command[1].startswith ('top')) or (command[1].startswith ('bottom')):
                    process_command(response)
                    print("\n")
                else:
                    print('Command not recognized: ' + response)

            elif command[0] == 'companies':
                if (command[1].startswith ('country')) or (command[1].startswith ('region')) or (command[1].startswith ('ratings')) or (command[1].startswith ('cocoa')) or (command[1].startswith ('bars_sold')) or (command[1].startswith ('top')) or (command[1].startswith ('bottom')):
                    process_command(response)
                    print("\n")
                else:
                    print('Command not recognized: ' + response)               

            elif command[0] == 'countries':
                if (command[1].startswith ('sellers')) or (command[1].startswith ('region')) or (command[1].startswith ('sources')) or (command[1].startswith ('ratings')) or (command[1].startswith ('cocoa')) or (command[1].startswith ('bars_sold')) or (command[1].startswith ('top')) or (command[1].startswith ('bottom')):
                    process_command(response)
                    print("\n")
                else:
                    print('Command not recognized: ' + response)  

            elif command[0] == 'regions':
                if (command[1].startswith ('sellers')) or (command[1].startswith ('sources')) or (command[1].startswith ('ratings')) or (command[1].startswith ('cocoa')) or (command[1].startswith ('bars_sold')) or (command[1].startswith ('top')) or (command[1].startswith ('bottom')):
                    process_command(response)
                    print("\n")
                else:
                    print('Command not recognized: ' + response)    
            else:    
                print('Command not recognized: ' + response)    

     
        if response == 'exit':
            print('bye')
            exit()

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
