from socket import *
import os
import threading
import sys
import time
import signal
from shutil import copyfile

preds = [None,None]
successor_1 = None
successor_2 = None
file = None
class PingServer(threading.Thread):   
    def __init__(self,peer_num,server):
        super(PingServer, self).__init__()
        self.peer_num = peer_num
        self.server = server
        self.lock = threading.Lock()      
    def run(self):
        global preds
        while True:
            conn, addr = self.server.recvfrom(2048)
            message = conn.decode().split()
            if message[0] == "request":
                server = socket(AF_INET, SOCK_DGRAM)
                message_send = "response " + str(self.peer_num)
                server.sendto(message_send.encode(), addr)
                print("A ping request message was received from Peer " + message[2] + ".")
                if message[1] == "1":
                    preds[0] = int(message[2])
                elif message[1] == "2":
                    preds[1] = int(message[2])
                print(preds)
class PingClient(threading.Thread):
    def __init__(self, peer_num,ping_interval):
        super().__init__()
        self.peer_num = str(peer_num)
        self.ping_interval = ping_interval
    def run(self):
        global successor_1
        global successor_2
        num_timeout_1 = 0
        num_timeout_2 = 0
        while True:
            clientSocket_1 = socket(AF_INET, SOCK_DGRAM)
            clientSocket_1.sendto(("request 1 " + self.peer_num).encode(), ("localhost", 12000 + int(successor_1)))
            clientSocket_1.settimeout(5)
            time.sleep(5)
            clientSocket_2 = socket(AF_INET, SOCK_DGRAM)
            clientSocket_2.sendto(("request 2 " + self.peer_num).encode(), ("localhost", 12000 + int(successor_2)))
            clientSocket_2.settimeout(5)
            print("Ping requests sent to Peers " + str(successor_1) + " and " + str(successor_2))
            
            try:
                message1, addr = clientSocket_1.recvfrom(2048)
                message1 = message1.decode().split()
                print("A ping " + message1[0] + " message was received from Peer " + message1[1] + ".")
            except timeout as e:
                num_timeout_1 += 1
            clientSocket_1.close()
          
            try:
                message2, addr = clientSocket_2.recvfrom(2048)
                message2 = message2.decode().split()
                print("A ping " + message2[0] + " message was received from Peer " + message2[1] + ".")
            except timeout as e:
                num_timeout_2 += 1
            clientSocket_2.close()
            
            if num_timeout_1 > 3:
                print("Peer " + str(successor_1) + " is no longer alive.")
                successor_1 = successor_2
                newPeer = socket(AF_INET, SOCK_STREAM)
                newPeer.connect(("localhost",12000 + successor_2))
                request = "notAlive"
                newPeer.send(request.encode())
                successor_2 = int(newPeer.recv(1024).decode())
                newPeer.close()
                num_timeout_1 = 0
                print("My first successor is now peer " + str(successor_1))
                print("My second successor is now peer " + str(successor_2))
            if num_timeout_2 > 3:
                print("Peer " + str(successor_2) + " is no longer alive.")
                newPeer = socket(AF_INET, SOCK_STREAM)
                newPeer.connect(("localhost",12000 + successor_1))
                request = "notAlive"
                newPeer.send(request.encode())
                successor_2 = int(newPeer.recv(1024).decode())
                newPeer.close()
                num_timeout_2 = 0
                print("My first successor is now peer " + str(successor_1))
                print("My second successor is now peer " + str(successor_2))
            time.sleep(self.ping_interval)

def get_file_position(filename,peer_num):
    position = filename % 256
    return position

def store_file(peer_num,position,filename):
    global file
    global successor_1
    file_socket = socket(AF_INET, SOCK_STREAM)
    file_socket.connect(("localhost",12000 + successor_1))
    
    if (peer_num > successor_1 and position > peer_num):
        message = "Save " + str(filename)
        file_socket.send(message.encode())
        file_socket.close()

        print("Store " + str(filename) +  " request forwarded to my successor")
    elif position == peer_num:
        print("Store " + str(filename) + " request accepted")
        file = filename
        file_socket.close()
    elif position < successor_1 and position > peer_num:
        message = "Save " + str(filename)
        file_socket.send(message.encode())
        file_socket.close()
        print("Store " + str(filename) +  " request forwarded to my successor")
    else:
        message = "Store " + str(filename)
        file_socket.send(message.encode())
        file_socket.close()
        print("Store " + str(filename) +  " request forwarded to my successor")


def get_file(filename,peer_num,successor_1,request_peer):
    global file
    if file == filename:
        print("File " + str(filename) + " is stored here")
        file_socket = socket(AF_INET, SOCK_STREAM)
        file_socket.connect(("localhost",12000 + request_peer))
        print("Sending file " + str(filename) + " to Peer " + str(request_peer))
        message = "Send from " + str(peer_num) + " "  + str(filename)
        file_socket.send(message.encode())
        print("The file has been sent")
    else:
        file_socket = socket(AF_INET, SOCK_STREAM)
        file_socket.connect(("localhost",12000 + successor_1))
        message = "Request " + str(filename) + "  from " + str(request_peer)
        file_socket.send(message.encode())
        print("File request for " + str(filename) + " has been sent to my successor")
    file_socket.close()


class TCP_server(threading.Thread):
    global preds
    def __init__(self, peer_num,server):
        super().__init__()
        self.peer_num = peer_num
        self.server = server
    def run(self):
        global successor_1
        global successor_2
        global file
        self.server.listen(1)
        while True:
            # if some item in preds is empty that means, current peer did not recieve the request from preds
            if (preds[0] == None or preds[1] == None):
                continue
            connectionSocket, addr = self.server.accept()
            message = connectionSocket.recv(1024)
            message = message.decode().split()
            if (message == []):
                continue
            if (message[0] == "Send"):
                print("Peer " + message[2] + " had File " + message[3])
                print("Receiving File " + message[3] + " from Peer " + message[2])
                copyfile(message[3] + ".pdf", "recieved_" + message[3] + ".pdf")
                print("File " + message[3] + " recieved")
            elif (message[0] == "notAlive"):
                response = str(successor_1)
                connectionSocket.send(response.encode())
            elif (message[1] == "join"):
                if ((int(message[0]) > self.peer_num and int(message[0]) < successor_1) or (self.peer_num > successor_1 and int(message[0]) > self.peer_num) or (self.peer_num > successor_1 and int(message[0]) < successor_1)):
                    newP_suc1 = successor_1
                    newP_suc2 = successor_2
                    successor_2 = successor_1
                    successor_1 = int(message[0])
                    print("Peer " + message[0] + " Join request received")
                    print("My new first successor is Peer " + message[0])
                    print("My new second successor is Peer " + str(successor_2))
                    # send accept response
                    response = "accept " + str(newP_suc1) + " " + str(newP_suc2)
                    accept_client = socket(AF_INET, SOCK_STREAM)
                    accept_client.connect(("localhost",12000 + int(message[0])))
                    accept_client.send(response.encode())
                    accept_client.close()
                    #send to predecessor to change 
                    TCPclient = socket(AF_INET, SOCK_STREAM)
                    TCPclient.connect(("localhost",12000 + preds[0]))
                    request = "Successor 2 Change " + message[0]
                    TCPclient.send(request.encode())
                    TCPclient.close()
                else:
                    print("Peer " + message[0] + " Join request forwarded to my successor ")
                    TCPclient = socket(AF_INET, SOCK_STREAM)
                    TCPclient.connect(("localhost",12000 + successor_1))
                    message = str(message[0]) + " join request"
                    TCPclient.send(message.encode())
                    TCPclient.close()
            elif (message[0:3] == ['Successor', '2', 'Change']):
                print("Successor Change request received")
                successor_2 = message[3]
                print("My new first successor is Peer " + str(successor_1))
                print("My new second successor is Peer " + str(successor_2))
            elif (message[0] == "quit"):
                depart_peer = int(message[1])
                print("Peer " + message[1] + " will depart from the network.")
                if successor_1 == depart_peer:
                    successor_1 = int(message[2])
                    successor_2 = int(message[3])                  
                else:
                    successor_2 = int(message[2])
                print("My new first successor is Peer " + str(successor_1))
                print("My new second successor is Peer " + str(successor_2))
            
            elif (message[0] == "Store"):
                filename = int(message[1])            
                position = get_file_position(filename,self.peer_num)
                store_file(self.peer_num,position,filename)
            elif (message[0] == "Save"):
                filename = int(message[1])  
                print("Store " + str(filename) + " request accepted")
                file = filename
            elif (message[0] == "Request"):
                filename = int(message[1])
                request_peer = int(message[3])
                get_file(filename,self.peer_num,successor_1,request_peer)
            
            connectionSocket.close()

def input_command(peer_num,successor_1,successor_2):
    global preds
    global file
    while True: 
        command = input()
        command = str.split(command)
        if command[0] == "Quit":
            time.sleep(10)
            quit_socket = socket(AF_INET, SOCK_STREAM)
            quit_socket.connect(("localhost", 12000 + preds[0]))
            quit_socket.sendall(("quit " + str(peer_num) + " " + str(successor_1) + " " + str(successor_2)).encode())
            quit_socket.close()          

            quit_socket = socket(AF_INET, SOCK_STREAM)
            quit_socket.connect(("localhost", 12000 + preds[1]))
            quit_socket.sendall(("quit " + str(peer_num) + " " + str(successor_1) + " " + str(successor_2)).encode())
            quit_socket.close()      
            os._exit(0)           
        
        elif command[0] == "Store":
            if len(command[1]) == 4 and command[1].isdigit():
                print(command)
                filename = int(command[1])            
                position = get_file_position(filename,peer_num)
                store_file(peer_num,position,filename)                 
            else:
                print("invalid filename")
        elif command[0] == "Request":
            filename = int(command[1])
            if len(command[1]) == 4 and command[1].isdigit():
                if (file == filename):
                    print("Peer " + str(peer_num) + " had File " + str(filename))
                else:
                    file_socket = socket(AF_INET, SOCK_STREAM)
                    file_socket.connect(("localhost",12000 + successor_1))
                    message = "Request " + str(filename) + "  from " + str(peer_num)
                    file_socket.send(message.encode())
                    print("File request for " + str(filename) + " has been sent to my successor")
                    result = file_socket.recv(1024).decode()
                    print(result)
                    file_socket.close()
            else:
                print("invalid filename")

def main(argv):
    if (argv[1] == "init"):
        if (len(argv) != 6):
            raise ValueError("invalid in put")
        peer_number = int(argv[2])
        successor_1 = int(argv[3])
        successor_2 = int(argv[4])
        ping_interval = int(argv[5])
        # udp server
        serversSocket = socket(AF_INET, SOCK_DGRAM)
        serversSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        serversSocket.bind(("localhost", 12000 + peer_number))
        server = PingServer(peer_number,serversSocket)
        server.start()
        # udp client
        client = PingClient(peer_number,ping_interval)
        client.start()
        # tcp server
        
        TCPSocket = socket(AF_INET, SOCK_STREAM)
        TCPSocket.bind(("localhost",12000 + peer_number))
        Tcp_server = TCP_server(peer_number,TCPSocket)
        Tcp_server.start()
        
        global successor_1
        global successor_2
        input_command(peer_number,successor_1,successor_2)
    
    elif (argv[1] == "join"):
        
        if (len(argv) != 5):
            raise ValueError("invalid in put")
        peer_number = int(argv[2])
        known_peer = int(argv[3])
        ping_interval = int(argv[4])

        
        serverPort = 12000 + known_peer
        join_Socket = socket(AF_INET, SOCK_STREAM)
        join_Socket.connect(("localhost",serverPort))
        message = str(peer_number) + " join request"
        join_Socket.send(message.encode())
        join_Socket.close()

        accept_socket = socket(AF_INET, SOCK_STREAM)
        accept_socket.bind(("localhost",12000 + peer_number))
        accept_socket.listen(1)
        connectionSocket, addr = accept_socket.accept()
        result = connectionSocket.recv(1024)
        result = result.decode().split()
        connectionSocket.close()
        #accept_socket.close()
        successor_1 = int(result[1])
        successor_2 = int(result[2])
        print("Join request has been accepted")
        print("My first successor is Peer " + str(successor_1))
        print("My second successor is Peer " + str(successor_2))
        
        serversSocket = socket(AF_INET, SOCK_DGRAM)
        serversSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        serversSocket.bind(("localhost", 12000 + peer_number))
        server = PingServer(peer_number,serversSocket)
        server.start()
        # udp client
        client = PingClient(peer_number,ping_interval)
        client.start()
        # tcp server
        
    
        Tcp_server = TCP_server(peer_number,accept_socket)
        Tcp_server.start()

        global successor_1
        global successor_2
        input_command(peer_number,successor_1,successor_2)
        
if __name__=="__main__":
    main(sys.argv)