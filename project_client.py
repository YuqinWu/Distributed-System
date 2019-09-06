'''
Project: Distributed System with get-set functions.
Team: Seven-White
Author: Yuqin Wu, Xiangda Bai
CNetID: yuqinwu, xdbai
'''
import zmq
import sys
import time
import json

ip = "127.0.0.1"
if len(sys.argv) == 2: 
	port = sys.argv[1]
	context = zmq.Context()

	socket1 = context.socket(zmq.REQ)

	socket1.connect("tcp://{}:{}".format(ip,port))

	type = input("Input your request type: 'get' or 'put':\n")
	while type not in ["get", "put"]:
		type = input("retry your input type: 'get' or 'put':\n")
	key = input("Input your key:\n")

	info = { }
	if type == "get":
		info = {"type": type, "payload": {"key":key}}

	elif type == "put":
		value = input("Input your value:\n")
		info = {"type":type, "payload":{"key":key, "value": value}}

	socket1.send_json(json.dumps(info))
	print(socket1.recv_json())

else:
	print("Usage: python3 project_client.py <port>")


