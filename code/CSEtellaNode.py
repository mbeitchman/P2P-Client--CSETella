# Marc Beitchman
# CSEP552 Spring 2013
# Assignment #3

import socket
import sys
import threading
import struct
import CSEtellaHelper
import time

# thread func for server component
def server_func(host_port,peers_connected_from,peers_connected_from_lock,peers_connected_to, peers_connected_to_lock, textblocks_observed, textblocks_observed_lock, messages_to_transmit, messages_to_transmit_lock,
        messages_cache, messages_cache_lock):

    TCP_IP = socket.gethostbyname(socket.gethostname())
    TCP_PORT = host_port
    BUFFER_SIZE = 23

    # create the server listening socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((TCP_IP,TCP_PORT))
        s.listen(5)
        print "server: listening on " + str(TCP_IP) + ":" + str(TCP_PORT) 
    except socket.error, e:
        print "Server: Unable to listen on " + str(TCP_IP) + ":" + str(TCP_PORT)
        SHUTDOWN_EVENT.set()
        return

    # loop listening for connections
    while True:

        # count the number of connected peers
        peers_connected_from_lock.acquire()
        num_connections = len(peers_connected_from)
        peers_connected_from_lock.release()

        # only allow 10 connections
        if num_connections < 10:

            try:
                conn, addr = s.accept()
                print "Server: accepted connection to " + str(addr[0]) + ":" +  str(addr[1])
                num_connections += 1

                # add the connection
                peers_connected_from_lock.acquire()
                peers_connected_from.append([addr[0],addr[1]])
                peers_connected_from_lock.release()

                # event to signal the other peer thread to stop
                stop_thread = threading.Event()

                # spawn a thread for getting messages and another for pulling messages on the new socket
                pull_thread = threading.Thread(target=pull_thread_func, args=(conn, addr[0], addr[1], peers_connected_to, peers_connected_to_lock, textblocks_observed, 
                                textblocks_observed_lock, messages_to_transmit, messages_to_transmit_lock, messages_cache, messages_cache_lock, 
                                peers_connected_from,peers_connected_from_lock, stop_thread,True))
                transmit_thread  = threading.Thread(target=transmit_thread_func, args=(conn, addr[0], addr[1], peers_connected_to, peers_connected_to_lock, messages_to_transmit, 
                    messages_to_transmit_lock, peers_connected_from,peers_connected_from_lock, stop_thread, True))

                pull_thread.start()
                transmit_thread.start()

            except socket.error, e:
                print "Server: Error in accept"
                continue

        # check for shutdown
        if SHUTDOWN_EVENT.is_set() == True:
            s.close()
            return

        # sleep a little before accepting a new connection
        time.sleep(5)

# thread func for connector component
def connector_func(peers_connected_to, peers_connected_to_lock, peers_connected_from,peers_connected_from_lock, textblocks_observed, textblocks_observed_lock, messages_to_transmit, messages_to_transmit_lock,
        messages_cache, messages_cache_lock):

    # bootstrap node
    TCP_IP = '128.208.2.88' 
    TCP_PORT = 5002

    # add the bootstrap node
    peers_connected_to_lock.acquire()
    peers_connected_to.append([TCP_PORT, TCP_IP, 0])
    peers_connected_to_lock.release()

    # loop to check for and add new connections
    while True:

        # count the number of connections
        num_connections = 0
        peers_connected_to_lock.acquire()
        for peer in peers_connected_to:
            if peer[2] == 1:
                num_connections = num_connections + 1
        peers_connected_to_lock.release()

        # try to connect to other peers if I have less than 10 connections
        if num_connections < 10:

            print "Connector: connections " + str(num_connections)
            for peer in peers_connected_to:

                # if not connected to this peer
                if peer[2] == 0:

                    try:
                        # connect to peer
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((peer[1], int(peer[0])))
                        s.settimeout(5)

                        # update peer to be connected
                        peers_connected_to.remove(peer)
                        peer[2] = 1
                        peers_connected_to.append(peer)
                        num_connections = num_connections + 1

                        # event to signal the other peer thread to stop
                        stop_thread = threading.Event()

                        # spawn a thread for getting messages and another for pulling messages on the new socket
                        pull_thread = threading.Thread(target=pull_thread_func, args=(s, peer[1], peer[0], peers_connected_to, peers_connected_to_lock, textblocks_observed, 
                            textblocks_observed_lock, messages_to_transmit, messages_to_transmit_lock, messages_cache, messages_cache_lock, peers_connected_from,peers_connected_from_lock, stop_thread))
                        transmit_thread = threading.Thread(target=transmit_thread_func, args=(s, peer[1], peer[0], peers_connected_to, peers_connected_to_lock, messages_to_transmit, 
                            messages_to_transmit_lock, peers_connected_from,peers_connected_from_lock, stop_thread))

                        pull_thread.start()
                        transmit_thread.start()

                        if num_connections > 10:
                            break

                    except:
                        print "Connector: error connecting to " + str(peer[1]) + ":" + str(peer[0]) 
                        continue

            time.sleep(2)

        if SHUTDOWN_EVENT.is_set() == True:
            try:
                s.close
            except:
                "Connector didn't have a socket to close"
            return

        time.sleep(2)
 
# thread func for transmission thread for a connection
def transmit_thread_func(s, TCP_IP, TCP_PORT, peers_connected_to, peers_connected_to_lock, messages_to_transmit, messages_to_transmit_lock, peers_connected_from,peers_connected_from_lock, stop_thread, incoming = False):

    connected_port_ip = str(TCP_IP) + ":" + str(TCP_PORT)
    log_prefix = "Transmit Thread(" + str(connected_port_ip) + "): "
    num_errors = 0

    while True:

        # if there are too many errors, close the connection and shutdown both threads
        if num_errors > 100:
            stop_thread.set()
            s.close()

            if incoming == True:
                peers_connected_from_lock.acquire()
                for peer in peers_connected_from:
                    if TCP_IP == peer[0] and TCP_PORT == peer[1]:
                        peers_connected_from.remove(peer)
                peers_connected_from_lock.release()
            elif incoming == False:
                peers_connected_to_lock.acquire()
                for peer in peers_connected_to:
                    if TCP_IP == peer[1] and TCP_PORT == peer[0]:
                        peers_connected_to.remove(peer)
                        peer[2] = 0
                        peers_connected_to.append(peer)
                peers_connected_to_lock.release()

            return

        # check if shutdown needed
        if SHUTDOWN_EVENT.is_set() == True or stop_thread.is_set() == True:
            s.close()
            return

        # send a ping
        ping = CSEtellaHelper.MessageGenerator.generate_ping(5)
        try:
          val = s.send(ping) 
          print log_prefix + "Sent a ping"
        except socket.error, e:
          print log_prefix + "Error in sending ping"
          num_errors += 1
          continue

        time.sleep(1)

        # send a query
        query = CSEtellaHelper.MessageGenerator.generate_query(5)
        print log_prefix + "Sent a query"
        try:
          val = s.send(query) 
        except socket.error, e:
          print log_prefix +  "Error in sending query"
          num_errors += 1
          continue

        time.sleep(1)

        # send out needed messages to transmit
        messages_to_transmit_lock.acquire()
        for message in messages_to_transmit:

            # send out pong or reply directly to a sender
            if message[0] == 1 or message[0] == 3 and message[1] == connected_port_ip:
                
                try:
                    val = s.send(message[2])
                except socket.error, e:
                    print log_prefix +  "Error in sending message"
                    num_errors += 1
                    continue
                
                messages_to_transmit.remove(message)
            
            # otherwise send out and decrement number of times to send
            elif message[1] != connected_port_ip:

                try:
                    val = s.send(message[2])
                except socket.error, e:
                    print log_prefix +  "Error in sending message"
                    num_errors += 1
                    continue
                
                message[3] = message[3] - 1
                messages_to_transmit.remove(message)
                if message[3] != 0:                    
                    messages_to_transmit.append(message)

        messages_to_transmit_lock.release()

        time.sleep(10)


# thread func for pull thread component for a connection
def pull_thread_func(s, TCP_IP, TCP_PORT, peers_connected_to, peers_connected_to_lock, textblocks_observed, textblocks_observed_lock, messages_to_transmit, 
        messages_to_transmit_lock, messages_cache, messages_cache_lock, peers_connected_from,peers_connected_from_lock, stop_thread, incoming = False):
    
    connected_port_ip = str(TCP_IP) + ":" + str(TCP_PORT)
    log_prefix = "Pull Thread(" + str(connected_port_ip) + "): "
    num_errors = 0

    # loop while listening to messages
    while True:

        # if there are too many errors, close the connection and shutdown both threads
        if num_errors > 1000:
            stop_thread.set()
            s.close()

            print log_prefix + "exiting"

            if incoming == True:
                peers_connected_from_lock.acquire()
                for peer in peers_connected_from:
                    if TCP_IP == peer[0] and TCP_PORT == peer[1]:
                        peers_connected_from.remove(peer)
                peers_connected_from_lock.release()
            elif incoming == False:
                peers_connected_to_lock.acquire()
                for peer in peers_connected_to:
                    if TCP_IP == peer[1] and TCP_PORT == peer[0]:
                        if peer in peers_connected_to:
                            peers_connected_to.remove(peer)
                            peer[2] = 0
                            peers_connected_to.append(peer)
                peers_connected_to_lock.release()

            return

        # check for shutdown
        if SHUTDOWN_EVENT.is_set() == True or stop_thread.is_set() == True:
            s.close()
            return

        # receive a header
        try:
            data = s.recv(23)
            if not data: 
                continue
        except socket.error, e:
            print log_prefix + "Timeout on recv"
            num_errors += 1
            continue

        # parse the message type
        try:
            message_type = CSEtellaHelper.MessageReader.get_message_type_from_header(data)
        except:
            print log_prefix + "Error parsing the message type"
            num_errors += 1
            continue

        # handle a ping
        if message_type == 0:

            print log_prefix + "Received ping"

            # check if the message is in the cache
            messages_cache_lock.acquire()
            if messages_cache.exists(CSEtellaHelper.MessageReader.unpack_header(data)) == False:
                
                messages_to_transmit_lock.acquire()
                try:
                    # generate pong and add it to queue
                    pong = CSEtellaHelper.MessageGenerator.generate_pong(data, HOST_PORT, HOST_IP, True)
                    messages_to_transmit.append([1, connected_port_ip, pong, 1])
                    
                    messages_cache.add(CSEtellaHelper.MessageReader.unpack_header(data))
                    
                    # add message to queue for forwarding
                    if CSEtellaHelper.MessageReader.get_message_ttl_from_header(data) > 0:
                    
                        message = CSEtellaHelper.MessageGenerator.prepare_to_forward(CSEtellaHelper.MessageReader.unpack_header(data))
                        peers_connected_to_lock.acquire()
                        messages_to_transmit.append([0, connected_port_ip, message, 3])
                        peers_connected_to_lock.release()
                except:
                    print log_prefix + "Error in prepariong message for reply and forwarding"
                    num_errors += 1

                messages_to_transmit_lock.release()
                
            messages_cache_lock.release()
      
        # handle a pong
        elif message_type == 1:

            print log_prefix + "Received pong"

            try:
                payload_size = CSEtellaHelper.MessageReader.get_payload_size_from_header(data)
            except:
                print log_prefix + "Error getting payload size from header"
                num_errors += 1
                continue

            # recieve the payload
            try:
                payload = s.recv(payload_size)
            except socket.error, e:
                print log_prefix + "Error in receiving pong payload"
                num_errors += 1
                continue

            try:
                port = CSEtellaHelper.MessageReader.get_port_from_payload(payload, payload_size)
                ip = CSEtellaHelper.MessageReader.get_ip_address_from_payload(payload, payload_size)
            except:
                print log_prefix + "Error getting ip:port from paylod"
                num_errors += 1
                continue

            # add to list of connections with the last value indicating we aren't connected to this address
            if HOST_IP != ip and HOST_PORT != port:
                peers_connected_to_lock.acquire()
                if any(port == peer[0] and ip == peer[1] for peer in peers_connected_to) == False:
                    peers_connected_to.append([port, ip, 0])
                peers_connected_to_lock.release()

        # handle a query
        elif message_type == 2:
            
            print log_prefix + "Received query"

            # check if it is in the cache
            messages_cache_lock.acquire()
            if messages_cache.exists(CSEtellaHelper.MessageReader.unpack_header(data)) == False:
                
                # generate reply and add to cache
                try:
                    messages_to_transmit_lock.acquire()
                    reply = CSEtellaHelper.MessageGenerator.generate_reply(data, HOST_PORT, HOST_IP, True)
                    messages_to_transmit.append([3, connected_port_ip, reply, 1])
                except:
                    messages_to_transmit_lock.release()
                    print log_prefix + "Error preparing reply to query"
                    num_errors += 1
                    continue        
                
                try:
                    messages_cache.add(CSEtellaHelper.MessageReader.unpack_header(data))
                    
                    # prepare to forward and add to queue
                    if CSEtellaHelper.MessageReader.get_message_ttl_from_header(data) > 0:
                    
                        message = CSEtellaHelper.MessageGenerator.prepare_to_forward(CSEtellaHelper.MessageReader.unpack_header(data))

                        messages_to_transmit.append([0, connected_port_ip, message, 3])
     
                    messages_to_transmit_lock.release()
                except:
                    print log_prefix + "Error preparing receive message for forwarding"
                    num_errors += 1
                    messages_to_transmit_lock.release()

            messages_cache_lock.release()

        # handle a reply
        elif message_type == 3:
            
            print log_prefix + "Received reply"
        
            try:
                payload_size = CSEtellaHelper.MessageReader.get_payload_size_from_header(data)
            except:
                print log_prefix + "Error getting payload size from header"
                num_errors += 1
                continue
            
            # get payload
            try:
                payload = s.recv(payload_size)
            except socket.error, e:
                print log_prefix + "Error in receiving query payload"
                num_errors += 1
                continue

            # add to list of observed items in the payload
            textblock =""
            try:
                textblock = CSEtellaHelper.MessageReader.get_text_block_from_payload(payload, payload_size) 
                port = CSEtellaHelper.MessageReader.get_port_from_payload(payload, payload_size)
                ip = CSEtellaHelper.MessageReader.get_ip_address_from_payload(payload, payload_size)
            except:
                print log_prefix + "Error getting textblock, ip and port from payload"
                num_errors += 1
                continue

            textblocks_observed_lock.acquire()
            if (textblock, ip, port) not in textblocks_observed:
                textblocks_observed.append((textblock, ip, port))
            textblocks_observed_lock.release()

        time.sleep(1)

# web server thread function
def webserver_func(peers_connected_to, peers_connected_to_lock, peers_connected_from, peers_connected_from_lock, 
    textblocks_observed, textblocks_observed_lock):

    TCP_IP = socket.gethostbyname(socket.gethostname())
    TCP_PORT = 20666

    # open the listening socket
    try:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.bind((TCP_IP, TCP_PORT))  
      sock.listen(5)
    except socket.error, e:
      print "Webserver: error creating listening socket on " + str(TCP_IP) + ":" + str(TCP_PORT)
      SHUTDOWN_EVENT.set()
      return 
    
    # accept and respond to requests
    while True:
      try:
        conn, addr = sock.accept()
      except socket.error, e:
          print "Webserver: error accepting connection"
          continue
      
      try:
        req = conn.recv(1024)
      except socket.error, e:
          print "Webserver: error receiving initial request"
          conn.close()
          continue

       # simply parse the request for a GET request and form the response
      if "GET" in req.upper():
        header = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n"
        response = header.encode()
        response += "<html><head> <title>CSEtella Node</title> </head> <body> <b>Marc's CSEtella Node</b><br/><br/> \
        Peers Connected To:<br/><br/>"
        
        peers_connected_to_lock.acquire()
        for peer in peers_connected_to:
            if peer[2] == 1:
                response +=  str(peer[1]) + ":" + str(peer[0]) + "<br/>"
        peers_connected_to_lock.release()

        response += "<br/>Peers Connected From:<br/><br/>"

        peers_connected_from_lock.acquire()
        for peer in peers_connected_from:
            response += str(peer[0]) + ":" + str(peer[1]) + "<br/>"
        peers_connected_from_lock.release()

        response += "<br/>Replies Observed:<br/>"
        textblocks_observed_lock.acquire()
        for textblock_observed in textblocks_observed:
            response += str(textblock_observed[0]) + " " + str(textblock_observed[1]) + ":" + str(textblock_observed[2]) + "<br/>"
        textblocks_observed_lock.release()

        uptime = time.time() - START_TIME

        response += "<br/>Up Time: " + str(uptime) + " seconds"

        response += "</body></html>"

      else:
        header = "HTTP/1.1 404 Not Found\r\nContent-Type: text/html\r\n\r\n"
        response = header.encode()
        response += "<html><head> <title>CSEtella Node</title> </head> <body> Unknown Request to Marc's CSETell Node</body> </html>"

      # send the response
      try:
        conn.sendall(response)
        print "web server sent a response"
      except  socket.error, e:
        print "Webserver: error sending response"
      
      # close the connection
      conn.close()

      # check for shutdown
      if SHUTDOWN_EVENT.is_set() == True:
        sock.close()
        return

# application entry point
if __name__ == "__main__":

    try:
        # parse the server port (note the webserver is hardcoded to port 20666)
        if len(sys.argv) != 2 or sys.argv[1].lower() == "-help":
          print "\nUsage: python CSEtella_node.py [host_portnumber] or python CSEtella_node.py -help"
          sys.exit(0)

        global SHUTDOWN_EVENT
        SHUTDOWN_EVENT = threading.Event()

        global HOST_PORT 
        HOST_PORT = int(sys.argv[1])

        global HOST_IP 
        HOST_IP = socket.gethostbyname(socket.gethostname())
        
        global START_TIME
        START_TIME = time.time()

        # global data for all threads
        peers_connected_to = []
        peers_connected_to_lock = threading.Lock()

        peers_connected_from = []
        peers_connected_from_lock = threading.Lock()

        textblocks_observed = []
        textblocks_observed_lock = threading.Lock()

        messages_to_transmit = []
        messages_to_transmit_lock = threading.Lock()

        messages_cache = CSEtellaHelper.CSETellaCache()
        messages_cache_lock = threading.Lock()

        # create and start main threads for the application
        server_thread = threading.Thread(target=server_func, args=(HOST_PORT,peers_connected_from,peers_connected_from_lock,peers_connected_to, peers_connected_to_lock, textblocks_observed, textblocks_observed_lock, messages_to_transmit, messages_to_transmit_lock,
            messages_cache, messages_cache_lock))
        connector_thread = threading.Thread(target=connector_func, args=(peers_connected_to, peers_connected_to_lock, peers_connected_from,peers_connected_from_lock,
            textblocks_observed, textblocks_observed_lock, messages_to_transmit, messages_to_transmit_lock, messages_cache, messages_cache_lock))
        webserver_thread = threading.Thread(target=webserver_func, args=(peers_connected_to, peers_connected_to_lock, peers_connected_from,peers_connected_from_lock,
            textblocks_observed, textblocks_observed_lock))

        server_thread.setDaemon(True)
        connector_thread.setDaemon(True)
        webserver_thread.setDaemon(True)

        server_thread.start()
        connector_thread.start()
        webserver_thread.start()

        # handle shutdown
        while True:
            if SHUTDOWN_EVENT.wait(10) == True:
                print "Main Thread: Unexpected shutdown raised..."
                time.sleep(30)
                print "Main Thread: Shutdown Completed..."
                break
    
    # handle shutdown
    except KeyboardInterrupt:
        SHUTDOWN_EVENT.set()
        print "Main Thread: Starting Shutdown...."
        time.sleep(30)
        print "Main Thread: Shutdown Completed..."

