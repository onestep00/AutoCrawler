import sqlite3
import random
from multiprocessing import Pool

db_path = "/home/jovyan/test/test.db"
# db_path = "../test.db"

print(sqlite3.sqlite_version)
conn = sqlite3.connect(db_path, isolation_level='EXCLUSIVE')
c = conn.cursor()
c.execute("DROP TABLE table_1")
c.execute("CREATE TABLE table_1 (id int,output int)")
conn.close()
# c.execute('INSERT INTO table_1 (id, output)' 'VALUES (?,?)', (index,output))


# @staticmethod
def write_db(x):
    conn = sqlite3.connect(db_path, isolation_level='EXCLUSIVE')
    c = conn.cursor()
    # c.execute("CREATE TABLE table_1 (id int,output int)")
    c.execute('INSERT INTO table_1 (id, output)' 'VALUES (?,?)', x)
    conn.commit()
    conn.close()
    
def exist_db(x):
    conn = sqlite3.connect(db_path, isolation_level='EXCLUSIVE')
    c = conn.cursor()
    # c.execute("CREATE TABLE table_1 (id int,output int)")
    c.execute('select EXISTS (select * from table_1 where output=?);', (x,))
    result = c.fetchone()[0]
#     print(result)
    conn.commit()
    conn.close()
    return True if result == 0 else False
    
def write_to_file(a_tuple):
    index = a_tuple[0]
    input = a_tuple[1]
    output = input + 1
    if exist_db(output):
        print("True")
        write_db((1,output))
    else:
        print("False")
#     conn = sqlite3.connect('/home/jovyan/test/test.db', isolation_level='EXCLUSIVE')
#     c = conn.cursor()
#     # c.execute("CREATE TABLE table_1 (id int,output int)")
#     c.execute('INSERT INTO table_1 (id, output)' 'VALUES (?,?)', (index,output))
#     conn.commit()
#     conn.close()

if __name__ == "__main__":
    p = Pool()
    l = []
    for i in range(100):
        l.append((i, random.randrange(1,20)))
    p.map(write_to_file, l)
    p.close()
    p.join()

    conn = sqlite3.connect(db_path, isolation_level='EXCLUSIVE')
    c = conn.cursor()
    c.execute("SELECT * FROM table_1")

    rows = c.fetchall()
    conn.close()
    print(rows)
    
