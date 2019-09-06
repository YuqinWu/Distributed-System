import zmq
context = zmq.Context()
pub_socket = context.socket(zmq.PUB)
pub_port = 6001
pub_socket.bind("tcp://127.0.0.1:%s" % pub_port)	
print("Publishing on :%s" % pub_port)
while True:
	pub_socket.send(input().encode())