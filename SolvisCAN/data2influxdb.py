#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 19:15:11 2018

@author: joe

"""
from influxdb import InfluxDBClient


idbclient = InfluxDBClient(host='localhost', port=8086)
idbclient.switch_database('phdata')


