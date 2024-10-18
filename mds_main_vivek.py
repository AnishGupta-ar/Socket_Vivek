from datetime import datetime
import threading
from multiprocessing import Process
# import redis
import json
import time
from connect_vivek import XTSConnect
from mds_client_vivek import MDSocket_io


# MarketData API Credentials

# Enter your api and secret key here


API_KEY = '9af31b94f3999bd12c6e89'
API_SECRET = 'Evas244$3H'

source = "WEBAPI"

# redis_ip = "172.16.47.54"
redis_ip = "127.0.0.1"
redis_port = 6379

# rr = redis.Redis(host=redis_ip, port=redis_port)
# rr.ping()

starttime = datetime.strptime("09:15", "%H:%M").time()
endtime = datetime.strptime("15:45", "%H:%M").time()

# Initialise
xt = XTSConnect(API_KEY, API_SECRET, source)

# Login for authorization token
response = xt.marketdata_login()
set_marketDataToken = response['result']['token']
set_muserID = response['result']['userID']
print("Login: ", response)

# Connecting to Marketdata socket
soc = MDSocket_io(set_marketDataToken, set_muserID)

# Instruments for subscribing
Instruments = [
    # {'exchangeSegment': 1, 'exchangeInstrumentID': 26009},
    # {'exchangeSegment': 1, 'exchangeInstrumentID': 26000},
    # {'exchangeSegment': 1, 'exchangeInstrumentID': 26001},
    {'exchangeSegment': 2, 'exchangeInstrumentID': 74686},
    {'exchangeSegment': 2, 'exchangeInstrumentID': 74687}
]


# for scrip in rr.keys():
#     try:
#         scrip = int(scrip.decode())
#
#         Instruments.append({'exchangeSegment': 2, 'exchangeInstrumentID': scrip})
#     except:
#         pass


# Callback for connection
def on_connect():
    """Connect from the socket."""
    print('Market Data Socket connected successfully!')

    # # Subscribe to instruments
    print('Sending subscription request for Instruments - \n' + str(Instruments))
    response = xt.send_subscription(Instruments, 1502)
    print('Sent Subscription request!')
    print("Subscription response: ", response)


# Callback on receiving message
def on_message(data):
    print('I received a message!')


# Callback for message code 1501 FULL
def on_message1501_json_full(data):
    print('I received a 1501 Touchline message!' + data)


# Callback for message code 1502 FULL
def on_message1502_json_full(data):

    # print("md data: ", data)
    #
    msg = json.loads(data)
    inst_id = msg.get('ExchangeInstrumentID')
    # Set the serialized JSON as the value associated with a key in Redis
    # rr.set(inst_id, data)
    print('1502 Market dept' + data)


# Callback for message code 1505 FULL
def on_message1505_json_full(data):
    print('I received a 1505 Candle data message!' + data)


# Callback for message code 1507 FULL
def on_message1507_json_full(data):
    print('I received a 1507 MarketStatus data message!' + data)


# Callback for message code 1510 FULL
def on_message1510_json_full(data):
    print('I received a 1510 Open interest message!' + data)


# Callback for message code 1512 FULL
def on_message1512_json_full(data):
    print('I received a 1512 Level1,LTP message!' + data)


# # Callback for message code 1105 FULL
def on_message1105_json_full(data):
    # pass
    print('I received a 1105, Instrument Property Change Event message!' + data)


# Callback for message code 1501 PARTIAL
def on_message1501_json_partial(data):
    print('I received a 1501, Touchline Event message!' + data)


# Callback for message code 1502 PARTIAL
def on_message1502_json_partial(data):
    print('I received a 1502 Market depth partial message!' + data)


# Callback for message code 1505 PARTIAL
def on_message1505_json_partial(data):
    print('I received a 1505 Candle data message!' + data)


# Callback for message code 1510 PARTIAL
def on_message1510_json_partial(data):
    print('I received a 1510 Open interest message!' + data)


# Callback for message code 1512 PARTIAL
def on_message1512_json_partial(data):
    print('I received a 1512, LTP Event message!' + data)


# # Callback for message code 1105 PARTIAL
# Callback for message code 1105 PARTIAL
def on_message1105_json_partial(data):
    print('I received a 1105, Instrument Property Change Event message!' + data)

# Callback for disconnection
def on_disconnect():
    print('Market Data Socket disconnected!')


# Callback for error
def on_error(data):
    """Error from the socket."""
    print('Market Data Error', data)


# Assign the callbacks.
soc.on_connect = on_connect
soc.on_message = on_message
soc.on_message1502_json_full = on_message1502_json_full
# soc.on_message1505_json_full = on_message1505_json_full
# soc.on_message1507_json_full = on_message1507_json_full
# soc.on_message1510_json_full = on_message1510_json_full
# soc.on_message1501_json_full = on_message1501_json_full
soc.on_message1512_json_full = on_message1512_json_full
soc.on_message1105_json_full = on_message1105_json_full
# soc.on_message1502_json_partial = on_message1502_json_partial
# soc.on_message1505_json_partial = on_message1505_json_partial
# soc.on_message1510_json_partial = on_message1510_json_partial
# soc.on_message1501_json_partial = on_message1501_json_partial
# soc.on_message1512_json_partial = on_message1512_json_partial
soc.on_message1105_json_partial = on_message1105_json_partial
soc.on_disconnect = on_disconnect
soc.on_error = on_error

# Event listener
el = soc.get_emitter()
el.on('connect', on_connect)
# el.on('1501-json-full', on_message1501_json_full)
el.on('1502-json-full', on_message1502_json_full)
# el.on('1507-json-full', on_message1507_json_full)
el.on('1512-json-full', on_message1512_json_full)
el.on('1105-json-partial', on_message1105_json_full)
# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
# soc.connect()

instruments_already_subscribed = []


# Define a function to be executed in parallel
def subscription_runtime():
    time.sleep(5)
    global instruments_already_subscribed

    # print("Entering into thread...")

    while True:
        if starttime <= datetime.now().time() <= endtime:
            try:

                # token = rr.get("websocket_token")
                # if token is None:
                #     print("No tokens found on redis Key")
                #     time.sleep(1.2)
                #     continue

                # token = json.loads(rr.get("websocket_token"))['token']
                # instruments_to_subscribe = rr.lrange("subscribed_instruments", 0, -1)
                instruments_to_subscribe  =[]
                if instruments_to_subscribe:

                    instruments_to_subscribe = [key.decode() for key in instruments_to_subscribe]

                    if set(instruments_to_subscribe) == set(instruments_already_subscribed):
                        time.sleep(1.0)
                        print("No New instruments to subscribe!")
                        continue

                    instr_token_list = []
                    for scripCode in instruments_to_subscribe:
                        if scripCode not in instruments_already_subscribed:
                            instr_token_list.append({"exchangeSegment": 2, "exchangeInstrumentID": str(int(scripCode))})
                            instruments_already_subscribed.append(scripCode)

                    instruments_already_subscribed = list(set(instruments_already_subscribed))

                    print("Subscribing...", instr_token_list)

                    xt.send_subscription(instr_token_list, 1502)

                else:
                    print("No tokens to subscribe!")
                time.sleep(0.5)
            except Exception as e:
                print(e)
                time.sleep(0.3)
        else:
            time.sleep(5)
            print("Time: ", datetime.now())

def socket_connection():
    flag = 0
    if flag:
        soc.connect()

    else:

        threading.Thread(target=soc.connect).start()
        print("Control Released!!!")


if __name__ == "__main__":
    p1 = Process(target=socket_connection)
    p1.start()

    p2 = Process(target=subscription_runtime)
    p2.start()

    p1.join()
    p2.join()
