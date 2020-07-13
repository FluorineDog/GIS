#!python
import psycopg2
binary_func_dict = {
    'within': ['within.csv', 'within.out', 'st_within.out'],
    'equals': ['equals.csv', 'equals.out', 'st_equals.out'],
    'distance': ['distance.csv', 'distance.out', 'st_distance.out'],
    'contains': ['contains.csv', 'contains.out', 'st_contains.out'],
    'intersects': ['intersects.csv', 'intersects.out', 'st_intersects.out'],
    'crosses': ['crosses.csv', 'crosses.out', 'st_crosses.out'],
    'overlaps':['overlaps.csv','overlaps.out', "st_overlaps.out"],  # error
    'touches':['touches.csv','touches.out', "st_touches.out"],  # error
    'hausdorffdistance':['hausdorff_distance.csv','hausdorff_distance.out','st_hausdorff_distance.out'],
    'distancesphere':['distance_sphere.csv','distance_sphere.out','st_distance_sphere.out'], # e
    'disjoint':['disjoint.csv', 'disjoint.out', "st_disjoint.out"],
}

bin_geo_func_dict = {
    'symdifference':['symmetric_difference.csv','symmetric_difference.out','st_symmetric_difference.out'],
    'difference':['difference.csv','difference.out', "st_difference.out"],
    'intersection': ['intersection.csv', 'intersection.out', 'st_intersection.out'],
    'union':['union.csv','union.out', "st_union.out"], 
}

all_dict = binary_func_dict.copy()
all_dict.update(bin_geo_func_dict)

conn = psycopg2.connect(database='mike', user='mike')
curs = conn.cursor()
curs.execute("""
create extension postgis;
""")

def init_table(): 
  curs.execute("""
  drop table if exists arctern;
  create table arctern(geos_left text, geos_right text);
  """)
  

def generate(sql, input_file, expected_file): 
  init_table()
  with open(input_file) as f:
      cmd = """COPY arctern FROM stdin with delimiter '|' csv HEADER"""
      curs.copy_expert(cmd, f)
  
  curs.execute(sql)
  results = curs.fetchall()
  with open(expected_file, 'w') as f:
    for row in results:
      assert(len(row) == 1)
      f.write(str(row[0]) + '\n')
    
      

  
for k, v in all_dict.items():
  name = "st_" + k 
  input_file = "../data/" +  v[0]
  expected_file = "../expected/st_" + v[1]
  # generate("st_" + k, "../data/within.csv", "../expected/st_within.out")
  # if k == "distance_sphere":
  # #   sql =  "{}(st_geomfromtext(geos_left), st_geomfromtext(geos_right))".format(name)
  # # else: 
  sql =  "{}(st_geomfromtext(geos_left), st_geomfromtext(geos_right))".format(name)   

  if k in bin_geo_func_dict:
    sql = "st_astext({})".format(sql)
  sql = "select {} from arctern".format(sql)
  generate(sql, input_file, expected_file)
