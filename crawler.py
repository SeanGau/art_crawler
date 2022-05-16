import requests, csv, asyncio
from bs4 import BeautifulSoup


all_data = []
url = 'https://grants.moc.gov.tw/Web/AnnounceName_List.jsp?P='
async def crawler(url):
    web = requests.get(url)
    soup = BeautifulSoup(web.text, "html.parser")
    for row in soup.find(class_='acceprSubsidyList').find_all('tr'):
        cols = row.find_all('td')
        if(len(cols) <= 0): continue
        current_data = {
            'date': cols[0].string,
            'title': cols[1].a.string,
            'link': cols[1].a['href'],
            'department': cols[2].string,
        }
        all_data.append(current_data)
        print(current_data)

tasks = []
for i in range(1, 31):
    task = asyncio.ensure_future(crawler(url+str(i)))
    tasks.append(task)

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(tasks))

print('data count: ', len(all_data))
with open('output.csv', 'w', newline='', encoding='utf-8-sig') as fo:
    fieldnames = ['date', 'title', 'link', 'department']
    writer = csv.DictWriter(fo, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_data)