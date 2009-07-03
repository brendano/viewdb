"""
Get a 1000-foot view of a big ol' pile of data.
Intended for when someone gives you an SQL dump and you're trying to figure out what's what.
Should show the schema, examples, and summary statistics.
"""

import util; util.fix_stdio()
import sys
import sqlalchemy
#db = sqlalchemy.create_engine('mysql://root@localhost/%s' % sys.argv[1])
if len(sys.argv) < 2:
  print>>sys.stderr, "Need DB schema as argument.  e.g. mysql://root@localhost/wordpress"
  sys.exit(1)
db = sqlalchemy.create_engine(sys.argv[1])
meta = sqlalchemy.MetaData(db, reflect=True)
table_names = meta.tables.keys()
table_names.sort()

def groupThousands(val, sep=','):
   # http://markmail.org/message/wykmd35ji77nc7ts
   if val < 0:
      return '-' + groupThousands(-val, sep)
   if val < 1000:
      return str(val)
   return '%s%s%03d' % (groupThousands(val // 1000, sep), sep, val % 1000)
  
def output(s):
  print util.unicodify(s)

def mean(x,y):
  return (x+y)*1.0 / 2

def truncate_at(s, max=40):
  s = util.unicodify(s)
  if len(s) > max:
    s = s[:max] + "&hellip;"
  return s

output("""<style>
body { font-size: 8pt; font-family: sans-serif }
td { font-size: 9pt;
 }
th { font-size: 9pt }
tr.top td { font-style: italic }
</style>
""")

table_counts = {}
for table_name in table_names:
  N = db.execute("select count(*) from %s" % table_name).fetchone()[0]
  table_counts[table_name] = N

output("<table>")
output("<tr><th>table <th>size")
for table_name in table_names:
  output("<tr><td><a href='#%s'>%s</a> <td align=right>%s" % (table_name, table_name, groupThousands(table_counts[table_name])))
output("</table>")

for table_name in table_names:
  output("<a name='%s'><h2>%s</h2></a>" % (table_name, table_name))
  
  table = meta.tables[table_name]
  N = table_counts[table_name]
  
  output("%s rows" % groupThousands(N))
  
  tn = table_name
  if N==0:
    sample_sqls = []
  elif db.url.drivername == 'mysql':
    ## can use LIMIT and OFFSETs
    sample_sqls = ["select * from %s limit 1" % tn]
    if N>1: sample_sqls += ["select * from %s limit 1 offset %d" % (tn, N-1) ]
    if N>2: sample_sqls[1:1] = ["select * from %s limit 1 offset %d" % (tn, int(N/2)) ]
  elif 'id' not in table.columns.keys():
    sample_sqls = []
  else:
    min_id = db.execute("select min(id) from %s" % tn).fetchone()[0]
    max_id = db.execute("select max(id) from %s" % tn).fetchone()[0]
    sample_sqls = ["select * from %s where id=%d" % (tn, min_id)]
    if N>1: sample_sqls += ["select * from %s where id=%d" % (tn, max_id)]
    middle = int(mean(min_id,max_id))
    if N>2: sample_sqls[1:1] = ["select * from %s where id>=%s order by id limit 1" % (tn,middle)]
  
  sample_rows = [db.execute(q).fetchone() for q in sample_sqls]
  
  output("<table cellspacing=0 cellpadding=1 border=1>")
  output("<tr class=top><td> <td>first row <td>middle row <td>last row")
  for colname in table.columns.keys():
    output("<tr><td><b>%s</b>" % colname)
    for row in sample_rows:
      if row is None:
        output("<td>")
        continue
      val = row[colname]
      if val is None: s = ""
      else: s = truncate_at(val)
      output("<td>%s" % s)
  output("</table>")


