import base64

import algosdk
from algosdk.v2client import algod
from algosdk.encoding import decode_address, encode_address
from algosdk import encoding
import msgpack

algod_address = "https://mainnet-algorand.api.purestake.io/ps2"
algod_token = 'oWqGS9gepoOUGq2axkfk3colf3zPybB3tlMoLik1'

headers = {
    "X-API-KEY": algod_token
}
LAST_ROUND = 0

# Set up the algod daemon client
algod_client = algod.AlgodClient(algod_token, algod_address, headers)


def decode_block():
    # Get the block in msgpack format
    block = algod_client.block_info(round_num=9000007, response_format='msgpack')
    # block_json = algod_client.block_info(round_num=9000007, response_format='json')
    # print(block_json)

    # Be sure to specify `raw=True` or msgpack will try to decode as utf8
    res = msgpack.unpackb(block)

    # Grabbing transactions from the block
    txns = res['block']['txns']

    # Iterating through block transactions
    for txn in txns:
        # Array to hold the transactions of 'pay' type
        pay_txns = []

        # Adding payment txns to pay_txns
        if txn['txn']['type'] == 'pay':
            amount = 0

            try:  # Checking to see if the payment transaction contains an 'amt' field (amount of payment)
                amount = txn['txn']['amt']
            except KeyError:
                print("This payment doesn't have an amount specified")

            fee = txn['txn']['fee']
            cost = amount + fee
            print("Transaction amount: " + str(amount) + " Transaction Fee: " + str(fee) + " Total Transaction Cost: " + str(cost) + " MicroAlgos\n")

    print(pay_txns)

    # Grab the byte value for the key `KEY`


def txn(block_data):
    transactions = block_data['block']['txns']
    for txn in transactions:
        print(txn)


def get_block_info(block):
    print(LAST_ROUND)
    block_info = (algod_client.block_info(round_num=block, response_format='msgpack'))
    return block_info


def init():
    global LAST_ROUND
    LAST_ROUND = algod_client.status().get('last-round')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    init()
    # txn(get_block_info(LAST_ROUND))
    decode_block();
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
