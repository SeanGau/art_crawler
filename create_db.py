import sqlite3

conn = sqlite3.connect('art_crawler.db')
print ("資料庫開啟")
c = conn.cursor()
c.execute('''
  CREATE TABLE ncafroc
  (hash  TEXT  NOT NULL,
   data  json  NOT NULL);
  ''')
c.execute('''
  CREATE TABLE moc
  (hash  TEXT  NOT NULL,
   data  json  NOT NULL);
  ''')
print ("資料庫創建成功")
conn.commit()
conn.close()