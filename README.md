# Distributed System Project

Requirement:
	This project used python3 and zmq package. Please make sure they are installed.

Server usage:
	$python3 project_server.py <client_port> <total_server> <my_order>(Starting from index 1)

Warning: for client port, please avoid using port start at 6000 because 6000 and latter ports will be used for intra-communication purpose. For example, with 5 servers, they will use 6000, 6001, 6002, 6003, 6004 for intra-communication.


Client usage:
	$python3 project_client.py <port>(this port must match one of the client_port of server)
	Then follow the prompt

	e.g:	
		$python3 project_server.py 1234 3 1
		(client port = 1234, total_server = 3, this is the first server)
		On a seperate terminal:
		$python3 project_server.py 1235 3 2
		(client port = 1235, total_server = 3, this is the second server)
		On a seperate terminal:
		$python3 project_server.py 1236 3 3
		(client port = 1236, total_server = 3, this is the third server)
		On a seperate terminal
		$python3 project_client.py 1235
		Input your request type: 'get' or 'put':
		put
		Input your key:
		a
		Input your value:
		1
		{"code": "Success"}




