# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 11:18:19 2018

@author: dep01158
"""
_VERSION_= '20181212.1'

import sys
import argparse

MQTT_FILE='D:/joe/python/mqtt-data/phdata-solvis-can-DLC8-2018-12-12.txt' # default

#sample mqtt messages:
mqtt_msgs= ['ts:2018-12-12 07:39:35; id:0x00000201; DLC:8; data:[0xCD,0x40,0xE3,0x02,0xEB,0xFF,0x0F,0x01]',
            'ts:2018-12-12 07:40:56; id:0x00000381; DLC:8; data:[0x25,0x02,0x89,0x00,0xE4,0x00,0x34,0x01]',
            'ts:2018-12-12 07:41:06; id:0x00000381; DLC:8; data:[0x0F,0x02,0x88,0x00,0xE4,0x00,0x55,0x01]',
            'ts:2018-12-12 07:41:06; id:0x00000281; DLC:8; data:[0x0D,0x01,0x00,0x00,0x1D,0x00,0x16,0x04]',
            'ts:2018-12-12 07:41:25; id:0x00000381; DLC:8; data:[0x04,0x02,0x88,0x00,0xE5,0x00,0x73,0x01]',
            'ts:2018-12-12 07:41:56; id:0x00000381; DLC:8; data:[0xEF,0x01,0x88,0x00,0xE5,0x00,0x91,0x01]',
            'ts:2018-12-12 07:43:06; id:0x00000381; DLC:8; data:[0xD1,0x01,0x88,0x00,0xE5,0x00,0xA2,0x01]',
            'ts:2018-12-12 07:43:14; id:0x00000301; DLC:8; data:[0x00,0x00,0xED,0x23,0xF2,0xFF,0xA4,0x01]']


#imestamp= datetime.strptime(msg.split(';')[0],'ts:%Y-%m-%d %H:%M:%S')
can_defs=[{'id':'0x00000201', 'payload':[{'name':'T-WoZi',           'bits':12, 'scale':10,   'unit': 'degC'},
                                         {'name':'T-WWpuffer',       'bits':12, 'scale':10,   'unit': 'degC'},
                                         {'name':'T-Koll',           'bits':12, 'scale':10,   'unit': 'degC'},
                                         {'name':'T-SolVL',          'bits':12, 'scale':10,   'unit': 'degC'}]},

          {'id':'0x00000281', 'payload':[{'name':'T-SolRL',          'bits':12, 'scale':10,   'unit': 'degC'},
                                         {'name':'Sol-Vol',          'bits':12, 'scale':1,    'unit': 'l/min'},
                                         {'name':'Sol-kWh.int',      'bits':12, 'scale':1e-3, 'unit': 'kWh'},   
                                         {'name':'Sol-kWh.fract',    'bits':16, 'scale':10,   'unit': 'kWh/10'}]},
                                        
          {'id':'0x00000301', 'payload':[{'name':'Sol-Pwr',          'bits':16, 'scale':1,    'unit': 'kW'},
                                         {'name':'Sol-Betr_h',       'bits':16, 'scale':1,    'unit': 'hours'},
                                         {'name':'T-Aussen',         'bits':12, 'scale':10,   'unit': 'degC'},
                                         {'name':'T-Ref',            'bits':12, 'scale':10,   'unit': 'degC'}]},
                                        
          {'id':'0x00000381', 'payload':[{'name':'T-LuftNachheizVL', 'bits':12, 'scale':10,   'unit': 'degC'},
                                         {'name':'T-MomoVL',         'bits':12, 'scale':10,   'unit': 'degC'},
                                         {'name':'undef-id:0x381.1', 'bits':12, 'scale':10,   'unit': 'degC'},
                                         {'name':'undef-id:0x381.2', 'bits':12, 'scale':10,   'unit': 'degC'}]}
        ]
       

def get_candef(id='', can_defs=[]):
    candef= [can_defs[i] for i in range(len(can_defs)) if can_defs[i]['id']==id] # search and return candef(id)
    if candef:
        return candef[0]
    else:
        return None
    
def uint_to_float(val, bits, scale):
    """
    calculate float from unsigned integer, using bits and scale.
        cut unsigned integer <val> to nr of <bits> and calc 2th complement if highest bit is set
        return resulting value divided by <scale> as float
    """
    assert val >= 0,    'uint_to_float(val={}, bits={}, scale={}) expects unsigned integer for parameter <val> !'.format(val,bits,scale)
    assert scale != 0,  'uint_to_float(val={}, bits={}, scale={}) expects non-zero value for parameter <scale> !'.format(val,bits,scale)
    assert bits > 0,    'uint_to_float(val={}, bits={}, scale={}) expects positiv value for parameter <bits> !'.format(val,bits,scale)
    mask= (2<<bits-1)-1
    val= val & mask
    neg_flag= (val > mask>>1)
    return (neg_flag*-(mask+1) +val)/scale #apply mask & 2th complement & scale


def decode_mqtt(mqtt_msgstr):
    """
    decode solvis canbus message, received via mqtt:
        
    """
    if  not mqtt_msgstr:
        return None
    
    """ condition mqtt string for decoding """
    mqtt_msgstr= mqtt_msgstr.replace('; ',';') #remove trailing blanks after ";" separator
    """ special to remove propblematic ":" from timestamp hour:minute:second part """
    timestamp_str=mqtt_msgstr.split(';')[0].split(':',1)[1] #original timestamp, containing ":"
    mqtt_msgstr= mqtt_msgstr.replace(timestamp_str,timestamp_str.replace(':','')) #removed ":" from timestamp
    
    mqtt_msg={k:v for k,v in (x.split(':') for x in mqtt_msgstr.split(';'))} #make dictionary from mqtt msg
    datastr= mqtt_msg['data'][1:-1].split(',') #extract data hex strings into array
    data_u8= [int(v,16) for v in datastr] #convert hexbyte strings to integer
    data_u16= [(data_u8[idx]) | (data_u8[idx+1]<<8) for idx in range(0,len(data_u8),2)] #combine LSB:u8-MSB:u8 -> u16
    candef= get_candef(mqtt_msg['id'], can_defs) # select can definition for current id
    
    """ special to recognize splitted defs in "'fract' and 'int' part
        example: ['Sol-kWh.int', 'Sol-kWh.fract']
        NOTE: current implementation can only handle one such item 
    """
    splitted_defs= [x['name'] for x in candef['payload'] if x['name'].split('.')[-1] in ['fract','int']]
    splitted_defs_unique= set([name.split('.')[0] for name in splitted_defs]) # 
    assert len(splitted_defs_unique) <=1, 'decode_mqtt() cannot handle multiple splitted defs within one msg: {}'.format(splitted_defs_unique) 

    decoded_msgs=[]
    for i in range(len(candef['payload'])):
        dataval= uint_to_float(data_u16[i],candef['payload'][i]['bits'],candef['payload'][i]['scale'])
        decoded_msgs.append({'ts':      timestamp_str,
                             'name':    candef['payload'][i]['name'], 
                             'dataval': dataval,
                             'unit':    candef['payload'][i]['unit']})
        
    splitted_items= [item for item in decoded_msgs if item['name'].split('.')[0] in splitted_defs_unique]
    if splitted_items:
        dataval_combined= sum([item['dataval'] for item in splitted_items])
        new_item= {'ts':splitted_items[0]['ts'],
                   'name':splitted_defs_unique.pop(),
                   'dataval': dataval_combined,
                   'unit':splitted_items[0]['unit']}
        for item in splitted_items:
            decoded_msgs.remove(item)
        decoded_msgs.append(new_item)
    return decoded_msgs


def data2val(data, bitlen, idx):
    mask= (2<<(bitlen-1)) -1
    val= data & mask
    if val >= (mask>>1): val= -(mask+1-val)
    return val

def main():
    parser = argparse.ArgumentParser(description='decode Solvis CAN msgs from mqtt, revision {}'.format(_VERSION_))
    parser.add_argument('--file','-f', type=str, default=MQTT_FILE,
                        help='(required) file containing Solvis CAN-bus mqtt messages to be analyzed')
    parser.add_argument('--Nmsgs','-N', type=int, default=0,
                    help='(optional) select mqtt message count')
    parser.add_argument('--listCANdefs','-C', action='store_true',default=False,
                        help='(list defined CAN message definitions')

    args = parser.parse_args()
    if args.listCANdefs:
        for candef in can_defs:
            print('id: {}:'.format(candef['id']))
            for payload in candef['payload']:
                print('\t{}'.format(payload))
        sys.exit()

    with open(args.file) as f:
        mqtt_msgs = f.read().splitlines()

    if args.Nmsgs:
        mqtt_msgs= mqtt_msgs[:args.Nmsgs]
        
    for CAN_msg in mqtt_msgs:
        dmsgs= decode_mqtt(CAN_msg)
        for dmsg in dmsgs:
            print('{} {:18s} {:7.1f} {}'.format(dmsg['ts'],dmsg['name'],dmsg['dataval'],dmsg['unit']))
        print()

if __name__ == "__main__":
  sys.exit(main())    
