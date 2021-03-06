import json
import urllib.error

import algosdk
from algosdk.v2client import algod
from algosdk.encoding import encode_address
import msgpack

algod_address = "https://mainnet-algorand.api.purestake.io/ps2"
algod_token = 'oWqGS9gepoOUGq2axkfk3colf3zPybB3tlMoLik1'  # Usually would hide this but in this case I decided not to

headers = {
    "X-API-KEY": algod_token
}
LAST_ROUND = 0

# Set up the algod daemon client
algod_client = algod.AlgodClient(algod_token, algod_address, headers)


def generate_block_message(block_num):
    # Get the block in msgpack format TODO: take block as an argument
    block = algod_client.block_info(round_num=block_num, response_format='msgpack')

    # Be sure to specify `raw=True` or msgpack will try to decode as utf8
    res = msgpack.unpackb(block, raw=True, strict_map_key=False)

    # Grabbing transactions from the block
    txns = res[b'block'][b'txns']

    message_list = {"block": str(block_num), "transactions": {}}
    transaction_number = 0

    # Iterating through block transactions
    for txn in txns:

        # Iterating through the transactions looking for type: pay
        if txn[b'txn'][b'type'] == b'pay':

            sender = encode_address(txn[b'txn'][b'snd'])
            receiver = None
            amount = 0
            fee = 0

            try:  # Checking to see if the payment transaction contains an 'amt' field (amount of payment)
                amount = txn[b'txn'][b'amt']
            except KeyError:
                continue

            try:  # Checking if transaction has a valid receiver address
                receiver = encode_address(txn[b'txn'][b'rcv'])
            except KeyError:
                continue

            try:
                fee = txn[b'txn'][b'fee']  # Grabbing fee from transaction
            except KeyError:
                continue

            message_list["transactions"][transaction_number] = {
                'sender': sender,
                'receiver': receiver,
                'transaction amount': amount,
                'fee': fee,
            }

            transaction_number = transaction_number + 1

    produced_message = json.dumps(message_list)
    return produced_message


def get_last_round():
    try:
        return algod_client.status().get('last-round')
    except urllib.error.HTTPError and algosdk.error.AlgodHTTPError:
        return get_last_round()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # txn(get_block_info(LAST_ROUND))
    generate_block_message();
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
