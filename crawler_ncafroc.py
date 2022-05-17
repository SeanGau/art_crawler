import requests, csv, asyncio, logging
from bs4 import BeautifulSoup

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "logfile.log",
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.ERROR)
logger = logging.getLogger()
loop = asyncio.get_event_loop()
all_data = []
async def crawler(url):
    web = await loop.run_in_executor(None, requests.get, url)
    soup = BeautifulSoup(web.text, "html.parser")
    for row in soup.find(class_='list-container').find_all(class_='media'):
        current_data = {
            'title': row.find(class_='media-body').h5.string,
            'link': row.a['href']
        }
        all_data.append(current_data)
        print(current_data)

tasks = []
for i in range(0, 139):
    task = asyncio.ensure_future(crawler('https://archive.ncafroc.org.tw/result/regular/list?size=50&page='+str(i)))
    tasks.append(task)
for i in range(0, 16):
    task = asyncio.ensure_future(crawler('https://archive.ncafroc.org.tw/result/project/list?size=50&page='+str(i)))
    tasks.append(task)
for i in range(0, 18):
    task = asyncio.ensure_future(crawler('https://archive.ncafroc.org.tw/result/international_cultural_exchange/list?size=50&page='+str(i)))
    tasks.append(task)

loop.run_until_complete(asyncio.wait(tasks)) #finish crawling list

print(len(all_data))

tasks = []
base_url = 'https://archive.ncafroc.org.tw'
async def crawler_page(raw_row):
    try:
        url = base_url+raw_row['link']
        print(url)
        web = await loop.run_in_executor(None, requests.get, url)
        soup = BeautifulSoup(web.text, "html.parser")
        table = soup.select_one('.pcShow .sidebar .block')
        grant = table.find(class_='n', string='年度期別').parent.select_one('.ctx p a').string.split(' ')
        if len(grant) < 3:
            grant = grant[0].split('年')
            grant.append('')
        if soup.select_one('.row-grantee .s-item .ctx p'):
            name = soup.select_one('.row-grantee .s-item .ctx p').string
        else:
            name = soup.select_one('.page-head h2').string
            
        current_data = {
            'title': raw_row['title'],
            'link': url,
            'category': table.find(class_='n', string='類別').parent.select_one('.ctx p a').string,
            'name': name.replace('\r\n', '').replace(' ', ''),
            'year': grant[0],
            'group': grant[1],
            'season': grant[2],
            'funding': table.find(class_='n', string='補助金額').parent.select_one('.ctx p').string.replace('元',''),
        }
    
        print(current_data)
        writer.writerow(current_data)
    except Exception as e:
        print(url, 'error! ', e)
        logger.error('{} {}'.format(url, e))


with open('output/final_output_ncafroc.csv', 'w', newline='', encoding='utf-8-sig', buffering=1) as fo:
    fieldnames = ['title', 'name', 'link', 'category', 'year', 'group', 'season', 'funding']
    writer = csv.DictWriter(fo, fieldnames=fieldnames)
    writer.writeheader()
    for row in all_data:
        task = asyncio.ensure_future(crawler_page(row))
        tasks.append(task)
        
    loop.run_until_complete(asyncio.wait(tasks))