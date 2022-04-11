import sqlite3

con = sqlite3.connect('DB.sqlite')
cur = con.cursor()

print('connection established')

cur.execute("create table if not exists accounts (acctID varchar(58) primary key, balance int);")
cur.execute("create table if not exists metadata (key varchar(20) primary key, value int  );")
con.commit()


def get_account_balance(account):
    cur.execute("select balance from accounts where acctID = ?", (account,))
    con.commit()
    try:
        ret = cur.fetchone()[0]
        return ret
    except TypeError:
        create_account(account)
        return 0


def create_account(account):
    try:
        cur.execute("insert into accounts (acctID, balance) values(?, ?)", (account, 0))
        con.commit()
    except sqlite3.IntegrityError:
        # Trying to create an account that already exists
        return


def update_balance(account, balance):
    cur.execute("update accounts set balance = ? where acctID = ?", (balance, account))
    con.commit()


def remove_from_balance(account, to_remove):
    starting_bal = get_account_balance(account)
    new_bal = starting_bal - to_remove
    update_balance(account, new_bal)


def add_to_balance(account, to_add):
    starting_bal = get_account_balance(account)
    new_bal = starting_bal + to_add
    update_balance(account, new_bal)


def get_current_block_progress():
    cur.execute("select value from metadata where key = 'block status'")
    con.commit()
    return cur.fetchone()[0]


def get_currently_processing():
    cur.execute("select value from metadata where key = 'processing'")
    con.commit()
    return cur.fetchone()[0]


def set_current_block_progress(block):
    cur.execute("update metadata set value = ? where key = 'block status'", (block,))
    con.commit()


def set_currently_processing(block):
    cur.execute("update metadata set value = ? where key = 'processing'", (block,))
    con.commit()


# This function really only exists for text purposes, there's no reason under the constraints to need this
def remove_account(account):
    cur.execute("delete from accounts where acctID = ?", (account,))
    con.commit()


def account_exists(account):
    cur.execute("select balance from accounts where acctID = ?", (account,))
    con.commit()
    ret = cur.fetchone()

    if ret is None:
        return False
