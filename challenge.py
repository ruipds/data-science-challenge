import psycopg2
import csv

fieldnames = ["transactionid",
                  "createdat",
                  "startdate",
                  "enddate",
                  "amountusd",
                  "status",
                  "revenuetype"]


try:
    conn = psycopg2.connect("dbname='datas' user='postgres' host='localhost' password='postgres'")
except:
    print "unable to connect to the database"

cur = conn.cursor()

cur.execute("""SELECT EXISTS (
   SELECT 1
   FROM   pg_tables
   WHERE  tablename = 'transactions'
   );""")
existsdb = cur.fetchall()
existsdb = existsdb[0][0]
if existsdb == False:
    print 'creating and populating table'
    cur.execute("""CREATE TABLE transactions (
                    transactionid   char(25), -- unique identifier
                    createdate       timestamp, -- when the invoice was issued
                    startdate       timestamp, -- start date of the period covered by the invoice
                    enddate         date, -- end date of the period covered by the invoice
                    amountusd       double precision, -- total amount of the invoice
                    status          varchar(12), -- can be 'issuable', 'issued', 'paid', 'not_invoiced', 'overdue', 'voided'
                    revenuetype     varchar(3) -- can be Subscription plan (SPC), Subscription plan Extra (SPE), One-time services (OTS), Top-up (TOP), Refunds (REF), Trial (TRI), Discounts (DIS)
                );""")
    with open("database.csv") as csvfile:
        reader = csv.DictReader( csvfile, fieldnames=fieldnames, delimiter=',', quotechar='|')
        for row in reader:
            if row['transactionid'] == 'Id':
                continue
            for f in fieldnames:
                if len(row[f]) == 0:
                    row[f] = None
            cur.execute("""INSERT INTO transactions VALUES (
                        %(transactionid)s,
                        %(createdat)s,
                        %(startdate)s,
                        %(enddate)s,
                        %(amountusd)s,
                        %(status)s,
                        %(revenuetype)s
                    )""", row)
    conn.commit()
else:
    print "table already exists"

cur.execute("""SELECT datesplit, sum(dayamount) FROM (
                      SELECT t.transactionid,createdate,COALESCE(t.startdate , t.createdate ) AS startdate,
                            COALESCE(t.enddate, t.createdate) AS enddate,t.amountusd, status, revenuetype, datesplit,
                            t.amountusd/(date_part('year', age(enddate, startdate))*365 +
                            date_part('month', age(enddate, startdate))*30 +
                            date_part('day', age(enddate, startdate))) as dayamount
                      FROM transactions AS t, generate_series('2017-01-01'::date, '2017-12-31'::date, interval '1d') AS datesplit
                      where date_part('year',createdate)=2017 AND status != 'voided' AND status != 'overdue' AND datesplit::date between startdate AND enddate
                  ) AS query group BY(datesplit) order BY(datesplit);
                """)
res = cur.fetchall()
file = open('results.csv','w+')
file.write("datesplit,sum\n")
for row in res:
    file.write("{0},{1}\n".format(row[0],row[1]))