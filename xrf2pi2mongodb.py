#!/usr/bin/python

import collections
import requests
import serial
import sys
import threading
import time

import pymongo

class MongoCurrentPublisher(object):

    def __init__(self, _name, database='nodeapp', collection='current'):

        self.coll = pymongo.Connection('localhost', 27017)[database][collection]
        self._name = _name

    def update(self, valuedict):
        self.coll.find_and_modify({'_name': self._name}, update=valuedict)


class SensorTxQueue(object):

    def __init__(self, fields, key, interval, url="http://api.thingspeak.com/update", mongo=None):
        self.master = {field:collections.deque() for field in fields}
        self.timeout = {}
        self.url = url
        self.key = key
        self.interval = interval
        self.mongo = mongo

    def add_value(self, field, value):
        self.renew_timeout(field)
        self.master[field].append(value)
        if self.all_sensors_ready():
            self.transmit()

    def all_sensors_ready(self):
        return all(map(bool, self.master.values()))

    def renew_timeout(self, field):
        try:
            self.timeout[field].cancel()
        except:
            pass
        self.timeout[field] = threading.Timer(self.interval, self.add_value, args=[field, float('nan')])
        self.timeout[field].start()

    def transmit(self):
        out = {}
        for field, queue in self.master.iteritems():
            try:
                out[field] = queue.popleft()
            except IndexError:
                print "[SQ] EE queue empty?"
        print out
        if self.mongo is not None:
            self.mongo.update(out)
        out['key'] = self.key
        r = requests.get(self.url, params=out)
        try:
            r.raise_for_status()
        except Exception, e:
            print "didnt work."
            print e.message

class XRFReadout(threading.Thread):

    def __init__(self, sensors_id2field, value_callback, battery_callback):
        self.dev = serial.Serial('/dev/ttyAMA0', baudrate=9600)
        
        self.sensors_id2field = sensors_id2field
        self.add_value = value_callback
        self.battery = battery_callback

        super(XRFReadout, self).__init__()

        self.daemon = True

    def run(self):

        buf = "" # string buffer

        while self.is_alive():
        
            N = self.dev.inWaiting()
            if N > 0:
                buf += self.dev.read(N)
            
            B = len(buf)
            if B > 0 and not buf.startswith("a"):
                try:
                    i = buf.index("a")
                except ValueError:
                    i = B
                else:
                    err, buf = buf[:i], buf[i:]
                    if err:
                        print "err"
                        # XXX
                        pass
            
            if B >= 12:
                # "aIITEMPXX.XX"
                while len(buf) >= 12:
                    packet, buf = buf[:12], buf[12:]
                    self.check_and_queue(packet)

            time.sleep(0.2)


    def check_and_queue(self, packet):
        if not packet.startswith("a"):
            print "Something wrong"
            return
        DEVID = packet[1:3]
        VALUE = packet[3:].strip('-')

        if VALUE.startswith("TMPA"):
            v = float(VALUE[4:])
            field = self.sensors_id2field[DEVID]
            self.add_value(field, v)
        elif VALUE.startswith("BATT"):
            v = float(VALUE[4:])
            field = self.sensors_id2field[DEVID]
            self.battery(field, v)
        else:
            # XXX
            pass


ID2FIELD = { '43' : 'field1',
             '44' : 'field2',
             '45' : 'field3',
             '46' : 'field4',
             '47' : 'field5' }





if __name__ == "__main__":

    import json

    # This loads the Thingspeak keys from a file using json
    keys = json.load(open('./thingspeak_keys.json'))
    TQ_KEY = keys['temperature']
    BQ_KEY = keys['battery']

    tp = MongoCurrentPublisher("temperature")
    bp = MongoCurrentPublisher("battery")
    queue = SensorTxQueue(ID2FIELD.values(), key=TQ_KEY, interval=65.)
    battqueue = SensorTxQueue(ID2FIELD.values(), key=BQ_KEY, interval=630.)

    work = XRFReadout(ID2FIELD, queue.add_value, battqueue.add_value)
    work.start()
    work.join()
