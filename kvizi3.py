import requests
import json
import sqlite3

url = "http://api.openweathermap.org/data/2.5/air_pollution/forecast"
API_key = 'b20151eb320136fa88be2d8ee864697b'
longitude = '116.4'
latitude = '39.9'#კოორდინატები ეკუთვნის ქალაქ პეკინს
payload = {'lat':latitude,'lon': longitude,'appid':API_key}
resp = requests.get(url,payload)
status = resp.status_code#გვიბრუნებს სერვერიდან მიღებული პასუხის სტატუსს
headers = resp.headers#სერვერიდან მიღებული პასუხის დამატებითი ინფორმაცია, რომელიც dict ტიპისაა და შეიცავს სერვერის დასახელებას,კონტენტის ტიპს და ა.შ.
text = resp.text#მოაქვს response-ს შიგთავსი ტექსტის სახით


#--------------------------------ვინახავ ფაილში json ფორმატის მონაცემს
with open('pollution.json','w') as file:
    json.dump(json.loads(text),file,indent=4)


#-------------------------------
file1 = open('pollution.json','r')
content = json.load(file1)
air_quality_index = content['list'][15]['main']['aqi']
co = content['list'][15]['components']['co']
pm10 = content['list'][15]['components']['pm10']
print(f"Air Pollution Info for Beijing, Date : 2021-05-01 15:00:00."
      f"Air quality index : {air_quality_index}, which means Poor,"
      f"amount of Co is : {co} μg/m3 and amount of pm10 : {pm10} μg/m3 ")
file1.close()

#--------------------------------
#ვქმნი ცხრილს, რომელშიც შემდეგი სვეტებია: id-autoincrement, date - გამოაქვს თარიღი და დრო unix timestampის მიხედვით,
#CO,PM10 და SO2 სვეტებში არსებული მონაცემები კი ასახავს ამ აირების რაოდენობას ჰაერში.
conn = sqlite3.connect('air_pollutionData.sqlite')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS pollution (id INTEGER PRIMARY KEY AUTOINCREMENT,
date INTEGER,CO FLOAT(30),PM10 FLOAT(30),SO2 FLOAT(30))''')


rows_list = []
for each in content['list']:
    date = each['dt']
    CO_amount = each['components']['co']
    PM10_amount = each['components']['pm10']
    SO2_amount = each['components']['so2']
    row = (date,CO_amount,PM10_amount,SO2_amount)
    rows_list.append(row)
    cursor.executemany('''INSERT INTO pollution (date,CO,PM10,SO2) VALUES (?,?,?,?)''', rows_list)

conn.commit()
conn.close()


