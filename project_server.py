'''
Project: Distributed System with get-set functions.
Team: Seven-White
Author: Yuqin Wu, Xiangda Bai
CNetID: yuqinwu, xdbai
'''

'''
 Client request message format: 
 {'type': 'put', 'payload': {'key': 'k', 'value': 'v'}}
 {'type': 'get', 'payload': {'key': 'k'}}
 
 request message format: 
 ALL, ALL, type(REQUEST/HEART/),content,term,requesterIP,requesterPort
 
 reply message format: 
 requesterIP,requesterPort,type(VOTE/ACK/),content,term
 
 content = phase&Log message format

 Log message format: 
 key&value
'''

import zmq
import sys
import time
import random
import json
import threading


# creating enumerations using class for 0 = follower, 1 = candidate, 2 = leader
FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

HEARTBEAT_TIMEOUT = 0.05
ELECTION_TIMEOUT_START = 0.15
ELECTION_TIMEOUT_END = 0.3
CLIENT_TIMEOUT = 5

class ServerMetadata:
	def __init__(self, Database, ip, firstPort, clientPort, myPubPort, serverCount, pubSocket, term, voted, MessageList, logQueue):
		threading.Thread.__init__(self)
		self.Database = Database
		self.ip = ip
		self.firstPort = firstPort
		self.clientPort = clientPort
		self.myPubPort = myPubPort
		self.serverCount = serverCount
		self.pubSocket = pubSocket
		self.term = term
		self.voted = voted
		self.MessageList = MessageList
		self.logQueue = logQueue
		self.LogList = []
		self.myRole = FOLLOWER
		self.leaderIP = None
		self.leaderPort = None

class ClientThread(threading.Thread):
	def __init__(self, serverData):
		threading.Thread.__init__(self)
		self.serverData = serverData
		
	def run(self):
		#This socket handle the communication with clients
		context = zmq.Context()
		clientSocket = context.socket(zmq.REP)
		clientSocket.bind("tcp://{}:{}".format(self.serverData.ip, self.serverData.clientPort))
		while True:
			receive = clientSocket.recv_json()
			message = json.loads(receive)
			if message["type"] =="get":
				clientSocket.send_json(self.getValue(message))
			else:
				# If no leader yet, simply return fail
				if self.serverData.leaderIP == None:
					print("no leader, sorry")
					clientSocket.send_json(json.dumps({"code": "Fail"}))
				# if I'm the leader now
				elif self.serverData.myRole == LEADER:
					payload = message["payload"]
					key = payload["key"]
					value = payload["value"]
					self.serverData.logQueue.append(key+"&"+value)
					deadline = time.time() + CLIENT_TIMEOUT
					succ = False
					while time.time() < deadline:
						if len(self.serverData.logQueue) == 0:
							clientSocket.send_json(json.dumps({"code": "Success"}))
							succ = True
							break
					if not succ:
						print("no response")
						clientSocket.send_json(json.dumps({"code": "Fail"}))

				# Else someone else is the leader. Redirect the message
				else:
					# Create the redirect Socket
					redirectsocket = context.socket(zmq.REQ)
					# Connect with the leader server
					redirectsocket.connect("tcp://{}:{}".format(self.serverData.leaderIP, self.serverData.leaderPort))
					# redirect the message to leader
					redirectsocket.send_json(receive)
					print("Redirecting...")
					# redirect back reply from leader to client.
					clientSocket.send_json(redirectsocket.recv_json())

	def getValue(self, message):
		payload = message["payload"]
		key = payload["key"]
		value = self.serverData.Database.get(key)
		if value:
			return json.dumps({"code":"Success", "payload":{"key":key, "value":value}})

		else:
			return json.dumps({"code":"Fail"})

class SubscriberThread(threading.Thread):
	def __init__(self, serverData, subPort, lock):
		threading.Thread.__init__(self)
		self.serverData = serverData
		self.subPort = subPort
		self.lock = lock
		
	def run(self):
		context = zmq.Context()
		subSocket = context.socket(zmq.SUB)
		subSocket.connect("tcp://%s:%s" % (self.serverData.ip, self.subPort))
		subSocket.setsockopt_string(zmq.SUBSCRIBE, "")
		print("Subscribed %s: %s"%(self.serverData.ip, self.subPort))
		while True:
			message = subSocket.recv().decode()
			#print("Received: %s" % message)
			headerIP, headerPort, data = message.split(",", 2)
			if headerIP == "ALL" or (headerIP == str(self.serverData.ip) and headerPort == str(self.serverData.clientPort)):
				self.lock.acquire()
				self.serverData.MessageList.append(data)
				self.lock.release()
	

def publishMessage(destIP, destPort, msgType, content, serverData, isRequest):
	reply = destIP + "," + destPort + "," + msgType + "," + content + "," + str(serverData.term)
	if isRequest:
		reply += "," + str(serverData.ip) + "," + str(serverData.clientPort)

	print("Sending mesage %s" %reply)
	serverData.pubSocket.send(reply.encode())


def follower(serverData):
	# voted = False
	receivedSignal = True
	print("I'm a follower in term %d!"%serverData.term)
	serverData.myRole = FOLLOWER
	while receivedSignal == True:
		print("FOLLOWER, counting down")
		# Getting a random timeout
		timeout = random.uniform(ELECTION_TIMEOUT_START, ELECTION_TIMEOUT_END)
		deadline = time.time() + timeout
		receivedSignal = False
		# Start counting time
		while time.time() < deadline:
			# When we detect a message, we process it
			if len(serverData.MessageList) > 0:
				msg = serverData.MessageList[0]
				serverData.MessageList.remove(msg)
				print("follower, received:%s"%msg)
				msgType, content, body = msg.split(",", 2)
				if msgType in ["REQUEST", "HEART"]:
					thisTerm, requesterIP, requesterPort = body.split(",", 2)
					# Filter out any message lower than current term.
					if int(thisTerm) >= serverData.term:
						if msgType == "REQUEST":					
							if int(thisTerm) > serverData.term or serverData.voted == False:
								serverData.leaderIP = None
								serverData.leaderPort = None
								serverData.term = int(thisTerm)
								serverData.voted = True
								publishMessage(requesterIP, requesterPort, "VOTE", "", serverData, False)
								receivedSignal = True
								break
						# Else this is a heartbeat msg.
						else:
							if int(thisTerm) > serverData.term:
								serverData.term = int(thisTerm)
								serverData.voted = False
							# Update my leader, my vote status if the term is updated
							serverData.leaderIP = requesterIP
							serverData.leaderPort = requesterPort
							phase, contentBody = content.split("&",1)
							replyContent = ""
							# We are ready to commit the change.
							if phase == "PHASE_1":
								replyContent = "READY"
							# It's time to commit the change to database
							elif phase == "PHASE_2":
								key, value = contentBody.split("&", 1)
								serverData.Database[key] = value
								serverData.LogList.append(contentBody)
								replyContent = "Committed"
							publishMessage(requesterIP, requesterPort, "ACK", replyContent, serverData, False)
							receivedSignal = True
							break
	return CANDIDATE


def candidate(serverData):
	print("I'm a candidate in term %d!"%(serverData.term+1))
	serverData.myRole = CANDIDATE
	serverData.leaderIP = None
	serverData.leaderPort = None
	voteCount = 1 
	majority = serverData.serverCount//2 + 1 
	while voteCount < majority:
		#Start requesting vote for next term.
		serverData.term += 1
		publishMessage("ALL", "ALL", "REQUEST", "VoteMe", serverData, True)
		print("CANDIDATE, counting down")
		# Getting a random timeout
		timeout = random.uniform(ELECTION_TIMEOUT_START, ELECTION_TIMEOUT_END)
		deadline = time.time() + timeout
		
		while time.time() < deadline:
			if len(serverData.MessageList) > 0:
				item = serverData.MessageList[0]
				serverData.MessageList.remove(item)
				print("candidate, received:%s"%item)
				msgType, content, body = item.split(",", 2)
				if msgType == "VOTE":
					msgTerm = body
					if int(msgTerm) == serverData.term:
						voteCount +=1
						if voteCount >= majority:
							return LEADER
					# Though I doubt this case never exist
					elif int(msgTerm) > serverData.term:
						serverData.voted = False
						return FOLLOWER
				elif msgType in ["REQUEST", "HEART"]:
					msgTerm, requesterIP, requesterPort = body.split(",", 2)
					if int(msgTerm) >= serverData.term:
						if msgType == "REQUEST":
							if int(msgTerm) > serverData.term:
								serverData.voted = True
								publishMessage(requesterIP, requesterPort, "VOTE", "", serverData, False)
								return FOLLOWER
						# Else if the message is a heartbeat. A leader exist.
						else:
							serverData.voted = False
							serverData.leaderIP = requesterIP
							serverData.leaderPort = requesterPort
							publishMessage(requesterIP, requesterPort, "ACK", "", serverData, False)
							return FOLLOWER
	return LEADER


def leader(serverData):
	print("I'm a leader in term %d!"%serverData.term)
	serverData.myRole = LEADER
	serverData.leaderIP = serverData.ip
	serverData.leaderPort = serverData.clientPort
	content = "&"
	voteCount = 1
	majority = serverData.serverCount//2 + 1
	currentPhase = 0
	while True: 
		publishMessage("ALL", "ALL", "HEART", content, serverData, True)
		if currentPhase == 0:
			if len(serverData.logQueue) != 0:
				content = "PHASE_1&"+serverData.logQueue[0]
				currentPhase = 1
			else: 
				content = "&"
		elif currentPhase == 1:
			if voteCount >= majority:
				currentPhase = 0
				voteCount = 1
				data = serverData.logQueue[0]
				key, value = data.split("&", 1)
				serverData.Database[key] = value
				content = "PHASE_2&" + data
				serverData.LogList.append(data)
				serverData.logQueue.remove(data)
			else:
				content = "&"

		for item in serverData.MessageList:
			print("LEADER, received:%s"%item)
			serverData.MessageList.remove(item)
			msgType, recvContent, body = item.split(",", 2)
			if msgType == "ACK":
				msgTerm = body
				if int(msgTerm) == serverData.term and recvContent == "READY":
					voteCount +=1

			elif msgType in ["REQUEST", "HEART"]:
				msgTerm, requesterIP, requesterPort = body.split(",", 2)
				# If the received message has a higher term, we need to handle it.
				if int(msgTerm) > serverData.term:
					if msgType == "REQUEST":
						serverData.voted = True
						serverData.leaderIP = None
						serverData.leaderPort = None
						reply = "VOTE"
					# Else it is a heartbeat.
					else:
						serverData.voted = False
						serverData.leaderIP = requesterIP
						serverData.leaderPort = requesterPort
						reply = "ACK"
					# Send the reply
					publishMessage(requesterIP, requesterPort, reply, "", serverData, False)
					# Then we return to follower.
					return FOLLOWER


		time.sleep(HEARTBEAT_TIMEOUT)

def main():
	if len(sys.argv) == 4:
		Database = {}
		Threads = []
		MessageList = []
		logQueue = []
		lock = threading.Lock()

		ip = "0.0.0.0"
		firstPort = 6000
		clientPort = int(sys.argv[1])
		serverCount = int(sys.argv[2])
		myPubPort = firstPort + int(sys.argv[3]) - 1
		context = zmq.Context()
		# This this is a single server configuration
		if serverCount == 1:
			clientSocket = context.socket(zmq.REP)
			clientSocket.bind("tcp://{}:{}".format(ip, clientPort))
			while True:
				reply = json.dumps({"code":"Fail"})
				receive = clientSocket.recv_json()
				message = json.loads(receive)
				payload = message["payload"]
				key = payload["key"]
				if message["type"] == "get":
					value = Database.get(key)
					if value:
						reply = json.dumps({"code":"Success", "payload":{"key":key, "value":value}})
				else:
					Database[key] = payload["value"]
					reply = json.dumps({"code":"Success"})
				clientSocket.send_json(reply)
		# Else this is a distributed system.
		else:
			# This socket handle the internal publish functionality
			pubSocket = context.socket(zmq.PUB)
			pubSocket.bind("tcp://%s:%s" % (ip, myPubPort))
			print("Publishing on %s:%s" % (ip, myPubPort))
			myServerData = ServerMetadata(Database, ip, firstPort, clientPort, myPubPort, serverCount, pubSocket, FOLLOWER, False, MessageList, logQueue)
			# This thread handle the interacting functionality with client
			clientThread = ClientThread(myServerData)
			clientThread.start()

			# These internal threads handle the leader election communications.
			for i in range(0, serverCount):
				channel = ''
				subPort = firstPort + i
				if subPort != myPubPort:
					subThread = SubscriberThread(myServerData, subPort, lock)
					Threads.append(subThread)
					subThread.start()

			#The server start running.
			nextRole = follower(myServerData)
			while True:
				if nextRole == FOLLOWER:
					nextRole = follower(myServerData)
				elif nextRole == CANDIDATE:
					nextRole = candidate(myServerData)
				elif nextRole == LEADER:
					nextRole = leader(myServerData)

	else:
		print("Usage: python3 project_server.py <client_port> <total_server> <my_order>(Starting from index 1)\ne.g: python3 project_server.py 1234 3 1")

if __name__=='__main__':
	main()
