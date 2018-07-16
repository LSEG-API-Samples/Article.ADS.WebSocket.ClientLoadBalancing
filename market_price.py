#!/usr/bin/env python
""" Simple example of outputting Market Price JSON data using Websockets """

import sys
import time
import getopt
import socket
import json
import websocket
import threading
from threading import Thread, Event

# Global Default Variables
hostname = '192.168.27.15'
port = '15000'
user = 'root'
app_id = '256'
position = socket.gethostbyname(socket.gethostname())

# Global Variables
web_socket_app = None
web_socket_open = False


def process_message(ws, message_json):
    """ Parse at high level and output JSON of message """
    message_type = message_json['Type']

    if message_type == "Refresh":
        if 'Domain' in message_json:
            message_domain = message_json['Domain']
            if message_domain == "Login":
                process_login_response(ws, message_json)
    elif message_type == "Ping":
        pong_json = { 'Type':'Pong' }
        ws.send(json.dumps(pong_json))
        print("SENT:")
        print(json.dumps(pong_json, sort_keys=True, indent=2, separators=(',', ':')))


def process_login_response(ws, message_json):
    """ Send item request """
    send_market_price_request(ws)


def send_market_price_request(ws):
    """ Create and send simple Market Price request """
    mp_req_json = {
        'ID': 2,
        'Key': {
            'Name': 'EUR=',
        },
    }
    ws.send(json.dumps(mp_req_json))
    print("SENT:")
    print(json.dumps(mp_req_json, sort_keys=True, indent=2, separators=(',', ':')))


def send_login_request(ws):
    """ Generate a login request from command line data (or defaults) and send """
    login_json = {
        'ID': 1,
        'Domain': 'Login',
        'Key': {
            'Name': '',
            'Elements': {
                'ApplicationId': '',
                'Position': ''
            }
        }
    }

    login_json['Key']['Name'] = user
    login_json['Key']['Elements']['ApplicationId'] = app_id
    login_json['Key']['Elements']['Position'] = position
    
    ########################################################################
    #Step 2: Add a DownloadConnectionConfig attribute in the login request #
    ########################################################################
    login_json['Key']['Elements']['DownloadConnectionConfig'] = 1
    ########################################################################
    #End Step 2                                                            #
    ########################################################################

    ws.send(json.dumps(login_json))
    print("SENT:")
    print(json.dumps(login_json, sort_keys=True, indent=2, separators=(',', ':')))


def on_message(ws, message):
    """ Called when message received, parse message into JSON for processing """
    print("RECEIVED: ")
    message_json = json.loads(message)
    print(json.dumps(message_json, sort_keys=True, indent=2, separators=(',', ':')))

    for singleMsg in message_json:
        process_message(ws, singleMsg)


def on_error(ws, error):
    """ Called when websocket error has occurred """
    print(error)


def on_close(ws):
    """ Called when websocket is closed """
    global web_socket_open
    print("WebSocket Closed")
    web_socket_open = False


def on_open(ws):
    """ Called when handshake is complete and websocket is open, send login """

    print("WebSocket successfully connected!")
    global web_socket_open
    web_socket_open = True
    send_login_request(ws)
#####################################################
#Step 4: Select a server with the least load factor #
#####################################################
def find_least_load_server(login_response):
    resp = json.loads(login_response)
    print(json.dumps(resp, sort_keys=True, indent=2, separators=(',', ':')))
    connectionconfig = resp[0]['Elements'].get('ConnectionConfig', None)
    if connectionconfig == None:
        return None
    else:
        entries = connectionconfig['Data']['Entries']
        if len(entries) == 0:
            return None
        else:
            least_hostname = None
            least_port = None
            least_factor = sys.maxsize
            for s in entries:                
                if s['Elements']['LoadFactor'] < least_factor:
                    least_hostname = s['Elements']['Hostname']
                    least_factor = s['Elements']['LoadFactor']                   
            print(least_hostname)
            return least_hostname    
########################################################
#End Step 4                                            #
########################################################

if __name__ == "__main__":

    # Get command line parameters
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["help", "hostname=", "port=", "app_id=", "user=", "position="])
    except getopt.GetoptError:
        print('Usage: market_price.py [--hostname hostname] [--port port] [--app_id app_id] [--user user] [--position position] [--help]')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("--help"):
            print('Usage: market_price.py [--hostname hostname] [--port port] [--app_id app_id] [--user user] [--position position] [--help]')
            sys.exit(0)
        elif opt in ("--hostname"):
            hostname = arg
        elif opt in ("--port"):
            port = arg
        elif opt in ("--app_id"):
            app_id = arg
        elif opt in ("--user"):
            user = arg
        elif opt in ("--position"):
            position = arg

    # Start websocket handshake
    ws_address = "ws://{}:{}/WebSocket".format(hostname, port)
    print("Connecting to WebSocket " + ws_address + " ...")

    ##########################################################
    #Step 1: Create another web socket connection to a server#
    ##########################################################
    web_socket_app = websocket.create_connection(ws_address,
                                header=['User-Agent: Python'],
                                subprotocols=['tr_json2'])
    send_login_request(web_socket_app)
    ##########################################################
    # End Step 1                                             #
    ##########################################################

    ######################################
    #Step 3:  Receive the login response #
    ######################################
    login_response = web_socket_app.recv()
    sel_server = find_least_load_server(login_response)
    print("The server that has the least load factor is "+sel_server)
    ######################################
    #End Step 3                          #
    ######################################

    ###########################################
    #Step 5: Reconnect to the selected server #
    ###########################################
    if sel_server != None:
        ws_address = "ws://{}:{}/WebSocket".format(sel_server, port)
    web_socket_app.close()
    print("Reconnect to "+ws_address)
    ###########################################
    #End Step 5                               #
    ###########################################

    web_socket_app = websocket.WebSocketApp(ws_address, header=['User-Agent: Python'],
                                        on_message=on_message,
                                        on_error=on_error,
                                        on_close=on_close,
                                        subprotocols=['tr_json2'])
    web_socket_app.on_open = on_open

    # Event loop
    wst = threading.Thread(target=web_socket_app.run_forever)
    wst.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        web_socket_app.close()
