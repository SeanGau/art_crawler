import requests, csv, asyncio, logging, functools, sqlite3, hashlib, json
from bs4 import BeautifulSoup

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "logfile.log",
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.ERROR)
logger = logging.getLogger()
loop = asyncio.get_event_loop()
conn = sqlite3.connect('art_crawler.db')

all_data = []
async def crawler(type, year):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.42'}
    committee_data = []
    try:
        url = 'https://www.ncafroc.org.tw/search_founding_list.html?searchType={}&keyword=&type={}&year={}&period=&categoryId=&itemId='.format('committee', type, year)
        web = await loop.run_in_executor(None, functools.partial(requests.get, url, headers=headers))
        print(url, 'get request!')
        soup = BeautifulSoup(web.text, "html.parser")
        rows = soup.find_all('tbody')
        for row in rows:
            data_title = {'year': '年度', 'season': '期數', 'type': '補助性質', 'category': '類別', 'chairman': '主席', 'members': '委員名單'}
            current_data = {}
            for key in data_title:
                if row.find('i', string=data_title[key]):
                    current_data[key] = row.find('i', string=data_title[key]).parent.find('span').get_text()
            if type != 'NORMAL':
                current_data['season'] = ''
            print(current_data)
            committee_data.append(current_data)
            
        # now crawl applicant
        url = 'https://www.ncafroc.org.tw/search_founding_list.html?searchType={}&keyword=&type={}&year={}&period=&categoryId=&itemId='.format('applicant', type, year)
        web = await loop.run_in_executor(None, functools.partial(requests.get, url, headers=headers))
        print(url, 'get request!')
        soup = BeautifulSoup(web.text, "html.parser")
        rows = soup.find_all('tbody')
        for row in rows:
            data_title = {'year': '年度', 'season': '期別', 'type': '補助性質', 'category': '類別', 'group': '項目', 'name': '申請者', 'title': '計畫名稱', 'funding': '補助經費', 'note': '備註'}
            current_data = {}
            for key in data_title:
                if row.find('i', string=data_title[key]):
                    current_data[key] = row.find('i', string=data_title[key]).parent.find('span').get_text().replace(' ','')
            for committee in committee_data:
                if committee['year'] == current_data['year'] and committee['season'] == current_data['season'] and committee['type'] == current_data['type'] and committee['category'] == current_data['category']:
                    current_data['committee'] = committee['chairman'].replace(' ','')+"、"+committee['members'].replace(' ','')
            print(current_data)
            all_data.append(current_data)
            

    except Exception as e:
        print('error! ', url, e)
        logger.error('{} {}'.format(url, e))

tasks = []
for year in range(85, 120):
    task = asyncio.ensure_future(crawler('NORMAL', year))
    tasks.append(task)
    task = asyncio.ensure_future(crawler('PROJECT', year))
    tasks.append(task)
    task = asyncio.ensure_future(crawler('COMMUNICATION', year))
    tasks.append(task)
loop.run_until_complete(asyncio.wait(tasks)) #finish crawling list

def makehash(rowdata):
    hashcode = hashlib.sha256(json.dumps(rowdata).encode('utf-8')).hexdigest()
    return hashcode

with open('output/final_output_ncafroc.csv', 'w', newline='', encoding='utf-8-sig', buffering=1) as fo:
    fieldnames = ['year', 'season', 'type', 'category', 'group', 'name', 'title', 'funding', 'note', 'committee']
    writer = csv.DictWriter(fo, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_data)
    c = conn.cursor()
    for row in all_data:
        hashcode = makehash(row)
        c.execute("select * from ncafroc where hash=?", [hashcode])
        if not c.fetchall():
            print("insert data", row)
            c.execute("insert into ncafroc values (?, ?, strftime('%s','now'))", [hashcode, json.dumps(row, ensure_ascii=False)])
    conn.commit()
    conn.close()