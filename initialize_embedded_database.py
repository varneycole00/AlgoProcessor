import sqlite3
import block_operations

con = sqlite3.connect('db.sqlite')
cur = con.cursor()


def init_db():
    cur.execute("create table if not exists accounts (acctID varchar(58) primary key, balance int);")
    cur.execute("create table if not exists metadata (key varchar(20) primary key, value int  );")
    last_network_block = block_operations.get_last_round()
    try:
        cur.execute("insert into metadata values ('block status', ?)", (last_network_block,))
    except sqlite3.IntegrityError:
        cur.execute("update metadata set value = ? where key = 'block status'", (last_network_block, ))
    con.commit()
    cur.close()
    con.close()


if __name__ == "__main__":
    init_db()
