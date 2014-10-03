#! /usr/bin/env python3
import paho.mqtt.client as pahoclient
import ssl, pprint, time,sys,errno,socket,argparse
from threading import Thread


def ssl_reconnect(self):
        """Reconnect the client after a disconnect. Can only be called after
        connect()/connect_async()."""
        if len(self._host) == 0:
            raise ValueError('Invalid host.')
        if self._port <= 0:
            raise ValueError('Invalid port number.')

        self._in_packet = {
            "command": 0,
            "have_remaining": 0,
            "remaining_count": [],
            "remaining_mult": 1,
            "remaining_length": 0,
            "packet": b"",
            "to_process": 0,
            "pos": 0}

        self._out_packet_mutex.acquire()
        self._out_packet = []
        self._out_packet_mutex.release()

        self._current_out_packet_mutex.acquire()
        self._current_out_packet = None
        self._current_out_packet_mutex.release()

        self._msgtime_mutex.acquire()
        self._last_msg_in = time.time()
        self._last_msg_out = time.time()
        self._msgtime_mutex.release()

        self._ping_t = 0
        self._state_mutex.acquire()
        ## Changed self._state = mqtt_cs_new
        self._state = 0

        self._state_mutex.release()
        if self._ssl:
            self._ssl.close()
            self._ssl = None
            self._sock = None
        elif self._sock:
            self._sock.close()
            self._sock = None

        # Put messages in progress in a valid state.
        self._messages_reconnect_reset()

        if self._tls_ca_certs is None:
            try:
                if (sys.version_info[0] == 2 and sys.version_info[1] < 7) or (sys.version_info[0] == 3 and sys.version_info[1] < 2):
                    self._sock = socket.create_connection((self._host, self._port))
                else:
                    self._sock = socket.create_connection((self._host, self._port), source_address=(self._bind_address, 0))
            except socket.error as err:
                ## Changed EAGAIN to erron.EAGAIN
                if err.errno != errno.EINPROGRESS and err.errno != errno.EWOULDBLOCK and err.errno != errno.EAGAIN:
                    raise
        else:
            context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(self._tls_ca_certs)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conn = context.wrap_socket(sock,server_hostname=self._host)
                conn.connect((self._host, self._port))
            except:
                print('Exception ',sys.exc_info()[0])
                raise

            self._sock = conn
            self._ssl = conn

        # if self._tls_ca_certs is not None:
        #     self._ssl = ssl.wrap_socket(
        #         self._sock,
        #         certfile=self._tls_certfile,
        #         keyfile=self._tls_keyfile,
        #         ca_certs=self._tls_ca_certs,
        #         cert_reqs=self._tls_cert_reqs,
        #         ssl_version=self._tls_version,
        #         ciphers=self._tls_ciphers)
        #
            if self._tls_insecure is False:
                if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 2):
                    self._tls_match_hostname()
                else:
                    ssl.match_hostname(self._ssl.getpeercert(), self._host)

        self._sock.setblocking(0)

        return self._send_connect(self._keepalive, self._clean_session)

def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))


def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def on_disconnect(client, userdata, rc):
    print('disconnect with ', rc,time.ctime())

def on_publish(client, userdata, mid):
    print('Published ',mid,time.ctime())

def on_log(client, userdata, level, buf):
    print('Logger  '+buf, time.ctime())

def on_subscribe(client, userdata, mid, granted_qos):
    print('Subscribe  ',mid,granted_qos)

def readSBS1FromHost(client):
    while True:
        try:
            flight_socket=socket.create_connection(('192.168.1.230',30003))
            for line in flight_socket.makefile('r'):
	            client.publish('/topic/flightinfo',line)
        except:
            print('Exception during read ',sys.exc_info()[0])
        print('Exiting read...reattaching in 20 seconds')
        time.sleep(20)





pahoclient.Client.reconnect = ssl_reconnect
oldReconnect = pahoclient.Client.reconnect

client = pahoclient.Client(client_id="1234", clean_session=False, userdata=None, protocol=3)
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log
client.on_disconnect = on_disconnect
client.on_publish = on_publish

# client.tls_set("/tmp/server.crt")
# client.username_pw_set('admin', 'admin123456')
# client.connect("amq2-noconnor.rhcloud.com", 2306)
client.connect("172.16.58.10", 1883)

worker = Thread(target=readSBS1FromHost,args=(client,))
worker.setDaemon(True)
worker.start()

client.loop_forever()

