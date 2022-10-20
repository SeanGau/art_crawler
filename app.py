from flask import Flask, jsonify, request, render_template
import sqlite3, json

print("sqlite version: ", sqlite3.sqlite_version)
app = Flask(__name__)

@app.route("/")
def root():
    return render_template("home.html")

@app.route("/api/getdata")
def getdata():
    args = request.args
    conn = sqlite3.connect('art_crawler.db')
    c = conn.cursor()
    sqlstring = "select data from ncafroc where 1=1"
    if args.get("year"):
        sqlstring += " and data->>'year'='{}'".format(args.get("year"))
    if args.get("type"):
        sqlstring += " and data->>'type'='{}'".format(args.get("type"))
    if args.get("title"):
        sqlstring += " and data->>'title' like '%{}%'".format(args.get("title"))
    
    print(sqlstring)
    c.execute(sqlstring)
    data = c.fetchmany(100)
    c.close()
    for index, row in enumerate(data):
        data[index] = json.loads(row[0])
    return jsonify(data)


if __name__ == "__main__":
	app.run(threaded=True, port=5000, debug=True)