import requests, csv, asyncio, datetime
from bs4 import BeautifulSoup

all_data = []
base_url = 'https://grants.moc.gov.tw/Web/'
async def crawler(raw_row):
    date = datetime.date.fromisoformat(raw_row['date'].replace("/","-"))
    if date < datetime.date(2018, 7, 27): return
    
    url = base_url+raw_row['link']
    print(url)
    web = requests.get(url)
    soup = BeautifulSoup(web.text, "html.parser")
    tables = []
    table_titles = soup.find_all(class_='title')
    for title in table_titles:
        tables.append({
            'title': title.string,
            'table': title.find_next_sibling("table")
        })
    brief_dict = {}
    grant_dict = {}
    judge_list = []
    for index in range(len(tables)):
        table_title = tables[index]['title']
        table = tables[index]['table']
        if index == 0: #breif
            for row in table.find_all('tr'):
                brief_dict[row.find('th').string.replace(":", "")] = row.find('td').string
            if '入圍' in brief_dict['發佈類別']: return
            print(brief_dict)
            
        elif '評審名單' in table_title:
            temp = table.find('th').string.replace(" ", "").replace("(依委員姓氏筆畫排序)", "")
            judge_list = "".join(temp.split()).split(",")
            print(judge_list)
            
        else:
            unit_list = []
            fieldnames = ['unit_name', 'unit_name', 'unit_fund', 'unit_remark']
            for row in table.find_all('tr'):
                cols = row.find_all('th')
                if len(cols) > 0: 
                    for index in range(len(cols)):
                        if '案件' in cols[index].string:
                            fieldnames[index] = 'unit_project'
                            break
                cols = row.find_all('td')
                if len(cols) <= 0: continue
                temp = {}
                for index in range(len(cols)):
                    temp[fieldnames[index]] = cols[index].string    
                unit_list.append(temp)
            grant_dict[table_title] = unit_list
            print(unit_list)
            
    for group in grant_dict:
        for unit in grant_dict[group]:
            out_dict = {
                'publish_year': brief_dict['公告年度'],
                'publish_date': brief_dict['發佈日期'],
                'publish_link': url,
                'grant_title': raw_row['title'],
                'grant_project': brief_dict['條款名稱'],
                'grant_department': brief_dict['受理單位'],
                'grant_judge': '、'.join(judge_list),
                'unit_group': group,
                'unit_name': unit['unit_name'],
                'unit_project': unit['unit_project'],
                'unit_fund': unit['unit_fund'],
                'unit_remark': unit['unit_remark'],
            }
            writer.writerow(out_dict)

tasks = []
with open('output.csv', 'r', newline='', encoding='utf-8-sig') as fi, open('final_output.csv', 'w', newline='', encoding='utf-8-sig', buffering=1) as fo:
    reader = csv.DictReader(fi)
    fieldnames = ['publish_year', 'publish_date', 'publish_link', 'grant_title', 'grant_project', 'grant_department', 'grant_judge', 'unit_group', 'unit_name', 'unit_project', 'unit_fund', 'unit_remark']
    writer = csv.DictWriter(fo, fieldnames=fieldnames)
    writer.writeheader()
    for row in reader:
        task = asyncio.ensure_future(crawler(row))
        tasks.append(task)
        
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))