# Marc Beitchman
# CSEP552 Spring 2013
# Assignment #3

import random
import struct
from collections import deque

# cache for storing processed messages implemented as a FIFO quee
class CSETellaCache:
  def __init__(self, limit = 30):
      self.data = deque()
      self.limit = limit

  def exists(self, message):
      message = self.get_message_id_and_type(message)
      if self.data.count(message) == 0:
        return False
      else:
        return True

  def message_id_exists(self, message):
    message_id = self.get_message_id(message)
    return any(self.get_message_id(value) == message_id for value in data)

  def add(self, message):
    if len(self.data) >= self.limit:
      self.data.popleft()

    message = self.get_message_id_and_type(message)
    self.data.append(message)

  def remove(self, message):

    self.data.remove(message)

  def get_message_id(self,message):

    return str(message[:15])

  def get_message_id_and_type(self,message):

    return str(message[:16])

# class for generating messages
class MessageGenerator:

  MESSAGE_TYPE_INDEX = 16
  TTL_INDEX = 17
  HOPS_INDEX = 18
  PAYLOAD_SIZE_INDEX = 19
  REPLY_TEXT = ['M','a','r','c',' ','B','e','i','t','c','h','m','a','n',' ','-','-',' ','m','b',
  'e','i','t','c','h','m','a','n','[','a','t',']','g','m','a','i','l','.','c','o','m',' ','-','-',' ','\\',
  'm','/',' ','\\','m','/']
  REPLY_TEXT_SIZE = 52

  # return a network byte order packed ping method
  @staticmethod
  def generate_ping(ttl):

     return struct.pack('!BBBBBBBBBBBBBBBBBBBI',random.randint(0,9),random.randint(0,9),random.randint(0,9),
      random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),
      random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),
      random.randint(0,9),0,ttl,0,0)

  @staticmethod
  def generate_pong(ping_header, port, ip,update_hops = False):

    unpacked_header = struct.unpack('!BBBBBBBBBBBBBBBBBBBI', ping_header)
    unpacked_header = list(unpacked_header)

    unpacked_header[MessageGenerator.MESSAGE_TYPE_INDEX] = 1
    unpacked_header[MessageGenerator.PAYLOAD_SIZE_INDEX] = 6
    if update_hops == True:
      unpacked_header[MessageGenerator.TTL_INDEX] -= 1
      unpacked_header[MessageGenerator.HOPS_INDEX] += 1
    
    unpacked_header.append(port)
    split_ip = ip.split('.')
    for val in split_ip:
      unpacked_header.append(int(val))

    return struct.pack('!BBBBBBBBBBBBBBBBBBBIHBBBB', *unpacked_header)

  # return a network byte order packed query method
  @staticmethod
  def generate_query(ttl):

    return struct.pack('!BBBBBBBBBBBBBBBBBBBI',random.randint(0,9),random.randint(0,9),random.randint(0,9),
      random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),
      random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),random.randint(0,9),
      random.randint(0,9),2,ttl,0,0)

  @staticmethod
  def generate_reply(query_header, port, ip, update_hops = False):

    unpacked_header = struct.unpack('!BBBBBBBBBBBBBBBBBBBI', query_header)
    unpacked_header = list(unpacked_header)

    unpacked_header[MessageGenerator.MESSAGE_TYPE_INDEX] = 3
    unpacked_header[MessageGenerator.PAYLOAD_SIZE_INDEX] = 6 + MessageGenerator.REPLY_TEXT_SIZE
    if update_hops == True:
      unpacked_header[MessageGenerator.TTL_INDEX] -= 1
      unpacked_header[MessageGenerator.HOPS_INDEX] += 1
    
    unpacked_header.append(port)
    split_ip = ip.split('.')
    for val in split_ip:
      unpacked_header.append(int(val))

    for val in MessageGenerator.REPLY_TEXT:
      unpacked_header.append(val)
  
    format_string = "!BBBBBBBBBBBBBBBBBBBIHBBBB"
    for x in range(0, MessageGenerator.REPLY_TEXT_SIZE):
      format_string += "c"
 
    return struct.pack(format_string, *unpacked_header)

  @staticmethod
  def prepare_to_forward(unpacked_header):

      unpacked_header = list(unpacked_header)

      unpacked_header[17] = int(unpacked_header[17]) - 1
      unpacked_header[18] = int(unpacked_header[18]) + 1

      return struct.pack('!BBBBBBBBBBBBBBBBBBBI', *unpacked_header)

# class for reading messages
class MessageReader:

  MESSAGE_TYPE_INDEX = 16
  TTL_INDEX = 17
  PAYLOAD_SIZE_INDEX = 19
  PAYLOAD_PORT_INDEX = 0

  # interpretation methods
  @staticmethod
  def unpack_header(header):

      return struct.unpack('!BBBBBBBBBBBBBBBBBBBI', header)

  @staticmethod
  def unpack_payload(payload, payload_size):

      format_string = "!HBBBB"
      for x in range(0, payload_size-6):
        format_string += "c"
      
      return struct.unpack(format_string, payload)

  @staticmethod
  def get_message_type_from_header(header):

      unpacked_header = struct.unpack('!BBBBBBBBBBBBBBBBBBBI', header)

      return unpacked_header[MessageReader.MESSAGE_TYPE_INDEX]

  @staticmethod
  def get_message_ttl_from_header(header):

      unpacked_header = struct.unpack('!BBBBBBBBBBBBBBBBBBBI', header)

      return unpacked_header[MessageReader.TTL_INDEX]

  @staticmethod
  def get_payload_size_from_header(header):
      
      unpacked_header = struct.unpack('!BBBBBBBBBBBBBBBBBBBI', header)

      return unpacked_header[MessageReader.PAYLOAD_SIZE_INDEX]

  @staticmethod
  def get_port_from_payload(payload, payload_size):
      
      format_string = "!HBBBB"
      for x in range(0, payload_size-6):
        format_string += "c"

      payload = struct.unpack(format_string, payload)

      return payload[0]
  
  @staticmethod
  def get_ip_address_from_payload(payload, payload_size):

    format_string = "!HBBBB"
    for x in range(0, payload_size-6):
      format_string += "c"

    payload = struct.unpack(format_string, payload)

    return str(payload[1]) + "." + str(payload[2]) + "." + str(payload[3]) + "." + str(payload[4]) 

  @staticmethod
  def get_text_block_from_payload(payload, payload_size):
    
    format_string = "!HBBBB"
    for x in range(0, payload_size-6):
      format_string += "c"

    payload = struct.unpack(format_string, payload)

    return "".join(list(payload[5:payload_size-1]))

