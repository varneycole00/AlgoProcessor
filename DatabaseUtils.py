import sqlite3

con = sqlite3.connect('DB.sqlite')
cur = con.cursor()

print('connection established')


def get_account_balance(account):
    cur.execute("select balance from accounts where acctID = ?", account)
    ret = cur.fetchone()
    if ret is None:
        create_account(account)
        return 0
    else:
        return ret


def create_account(account):
    cur.execute("insert into accounts (acctID, balance) values(?, 0)", account)


def update_balance(account, balance):
    cur.execute("update accounts set balance = ? where acctID = ?", (balance, account))


def remove_from_balance(account, to_remove):
    starting_bal = get_account_balance(account)
    new_bal = starting_bal - to_remove
    update_balance(account, new_bal)


def add_to_balance(account, to_add):
    starting_bal = get_account_balance(account)
    new_bal = starting_bal + to_add
    update_balance(account, new_bal)
