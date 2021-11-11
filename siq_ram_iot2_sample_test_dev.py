import logging
import json
import os
import sys
import boto3
import pymysql
# from datetime import datetime
import datetime
from struct import unpack
import base64
import zlib
import siteiq_ram_iot
import decimal
import array
from struct import *
# from datetime import timedelta
import random
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from decimal import Decimal
import base64
import paho.mqtt.client as mqtt_client


logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client('ssm')

rds_host_parameter = ssm.get_parameter(
    Name=os.environ['siq_ram_rdshost_param'])
db_siteiqram_parameter = ssm.get_parameter(
    Name=os.environ['siq_ram_dbsiteiqram_param'])
db_username_parameter = ssm.get_parameter(
    Name=os.environ['siq_ram_dbusername_param'], WithDecryption=True)
db_password_parameter = ssm.get_parameter(
    Name=os.environ['siq_ram_dbpassword_param'], WithDecryption=True)
s3_bucket_parameter = ssm.get_parameter(
    Name=os.environ['siq_ram_iotmessagesfolder_param'])

rdshost = rds_host_parameter['Parameter']['Value']
database = db_siteiqram_parameter['Parameter']['Value']
username = db_username_parameter['Parameter']['Value']
password = db_password_parameter['Parameter']['Value']
s3bucket = s3_bucket_parameter['Parameter']['Value']



class ThingTopic:
    def __init__(self):
        logger.info(
            "INFO: ThingTopic [siq_iot2_event_test] init")

        self.topic_name = "Twowire"
        self.message_type = "Two wire"
        self.item_type_Price = "Price Change Data"
        self.item_type_Transaction_Details = "Transaction Details"
        self.item_type_Transaction_Status = "Transaction Status"
        self.target_table_Price = "data_price"
        self.target_table_Transaction_Details = "data_transaction"
        self.target_table_event_pump = "event_pump"
        self.target_table_crind_status = "crind_status"
        self.target_table_crind_hardware = "crind_hardware"
        self.target_table_crind_software = "crind_software"
        self.target_table_data_transmission_statistics = "data_transmission_statistics"
        self.topic_message_data_type = "UD"  # Uncompressed Data
        self.topic_message_content_type = "Hex"  # Text data without cmd in header
        self.code_message_status_success = "S"
        self.code_message_status_failure = "F"
        self.code_message_status_partial = "P"
        self.code_item_status_success = "S"
        self.code_item_status_failure = "F"
        self.code_item_status_duplicate = "D"
        self.label_status_message_dispenser_not_found = "Error: Device IMEI to Dispenser mapping is not found."
        self.label_status_message_topic_data_not_found = "Error: Topic Message Header or Message Payload is not found."
        self.label_status_message__data_price_not_found = "Error: Data Price is not found."
        self.label_status_message__data_transaction_not_found = "Error: Data Transaction is not found."
        self.label_status_message__event_pump_not_found = "Error: Event Pump data is not found."
        self.label_status_message_data_price_duplicate = "Error: Data Price received from the device is a duplicate."
        self.label_status_message_data_transaction_duplicate = "Error: Data Transaction received from the device is a duplicate."
        self.label_status_message_event_pump_duplicate = "Error: Event Pump received from the device is a duplicate."
        self.label_status_message_exception = "Error: Topic Message parsing or data insert error."
        

    def open_db_connection(self):
        try:
            self.db_con = siteiq_ram_iot.get_db_connection(
                rdshost, username, password, database, 5, pymysql.cursors.DictCursor)
            logger.info(
                "INFO: ThingTopic [siq_iot2] Database <" + database + "> Connected")
        except Exception as e:
            logger.error(
                "ERROR: Unexpected error: Could not connect to MySQL instance.")
            logger.error(e)
            sys.exit()

    def close_db_connection(self):
        siteiq_ram_iot.close_db_connection(self.db_con)
        logger.info(
            "INFO: ThingTopic [siq_iot2_event_test] Database <" + database + "> Connection Closed")
            
    # def get_thing_message_log_status(self, thing_message_log, message_data_count, message_data_success_count, message_data_failure_count, message_data_duplicate_count):
    #     if (thing_message_log is not None):
    #         if (message_data_count == message_data_success_count):
    #             thing_message_log.update({"message_status": self.code_message_status_success, "status_message": (
    #                 "Total: " + str(message_data_count) + ", Success: " + str(message_data_success_count))})

    #         elif (message_data_count == message_data_failure_count or message_data_count == message_data_duplicate_count):
    #             thing_message_log.update({"message_status": self.code_message_status_failure, "status_message": ("Total: " + str(message_data_count) + ", Success: " + str(
    #                 message_data_success_count) + ", Failure: " + str(message_data_failure_count) + ", Duplicate: " + str(message_data_duplicate_count))})

    #         else:
    #             thing_message_log.update({"message_status": self.code_message_status_partial, "status_message": ("Total: " + str(message_data_count) + ", Success: " + str(
    #                 message_data_success_count) + ", Failure: " + str(message_data_failure_count) + ", Duplicate: " + str(message_data_duplicate_count))})

    #         return thing_message_log
    #     else:
    #         return None
            
    def topic_info(self, event):
        
        logger.info(
            "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - START")
            
        self.open_db_connection()    
        
        dispenser_id = None
        start_timestamp = None
        end_timestamp = None
        dispenser_timestamp = None
        device_timestamp = None
        receive_timestamp = None
        pump_number = None
        grade_number = None
        price_type = None
        price_value = None
        fuel_volume = None
        status_desc = None
        event_code = None
        flow_rate = None
        transaction_amount = None
        start_timestamp = None
        end_timestamp = None
         
        if 'A' in event:
            imei = event["A"].split('/')[0]
            dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
            if event["A"].split('/')[1] == "pu" and event["A"].split('/')[2] == "dt":
                
                if 'Am' in event:
                    transaction_amount = event['Am']
                    
                    if 'B' in event:
                        unix_timestamp = event['B']
                        device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    
                    if 'Pu' in event:
                        pump_number = event['Pu']
                    
                    if 'Gr' in event:
                        grade_number = event['Gr']
                        
                    if 'PU' in event:
                        price_value = event['PU']
                    
                    if 'Vo' in event:   
                        fuel_volume = event['Vo']
                        
                    if 'Ts' in event:
                        start_timestamp = event['Ts']
                        start_timestamp = datetime.datetime.utcfromtimestamp(int(start_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                        
                    if 'Te' in event:
                        end_timestamp = event['Te']
                        end_timestamp = datetime.datetime.utcfromtimestamp(int(end_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                        
                    datetimeFormat = '%Y-%m-%d %H:%M:%S'
                    diff = (datetime.datetime.strptime(end_timestamp, datetimeFormat)\
                    - datetime.datetime.strptime(start_timestamp, datetimeFormat)).total_seconds()
                    
                    
                    if (fuel_volume == 0):
                        flow_rate = 0
                    elif diff == 0:
                        flow_rate = 0
                    else:
                        flow_rate = (float(fuel_volume)/float(diff)) * 60
                        
                        
                    failure_array  = []    
            
            
                    if (device_timestamp is None):
                        failure_array.append(
                            "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                    else:
                        input_date = str(device_timestamp)
                        input_date = input_date.split(' ')
                        year, month, day = input_date[0].split('-')
                        isValid = True
                        try:
                            datetime.datetime(int(year),int(month),int(day))
                        except:
                            isValid = False  
                        if (isValid == False):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]") 
            
                    if(pump_number is None):
                        failure_array.append(
                            "Value: " + str(pump_number) + " [Issue: pump number is blank]")
                    else:
                        pump_number = int(pump_number)
            
                    if(fuel_volume is not None):
                        fuel_volume = float(fuel_volume)
                        
                    if(price_value is not None):
                        price_value = float(price_value)
                        
                    if(transaction_amount is not None):
                        transaction_amount = float(transaction_amount)
                        
                    if(flow_rate is not None):
                        flow_rate = float(flow_rate)    
                        
                    if (start_timestamp is None):
                        failure_array.append(
                            "Value: " + str(start_timestamp) + " [Issue: Start Timestamp is blank]")
                    else:
                        input_date = str(start_timestamp)
                        input_date = input_date.split(' ')
                        year, month, day = input_date[0].split('-')
                        isValid = True
                        try:
                            datetime.datetime(int(year),int(month),int(day))
                        except:
                            isValid = False  
                        if (isValid == False):
                            failure_array.append(
                                "Value: " + str(start_timestamp) + " [Issue: Start Timestamp is not valid]")
                                
                    if (end_timestamp is None):
                        failure_array.append(
                            "Value: " + str(end_timestamp) + " [Issue: End Timestamp is blank]")
                    else:
                        input_date = str(end_timestamp)
                        input_date = input_date.split(' ')
                        year, month, day = input_date[0].split('-')
                        isValid = True
                        try:
                            datetime.datetime(int(year),int(month),int(day))
                        except:
                            isValid = False  
                        if (isValid == False):
                            failure_array.append(
                                "Value: " + str(end_timestamp) + " [Issue: End Timestamp is not valid]")            
                        
                 
                    if (len(failure_array) == 0):
                        message_item = {
                            "dispenser_id": dispenser_id,
                            "pump_number": pump_number,
                            "grade_number": grade_number,
                            "price_type": price_type,
                            "price_value": price_value,
                            "fuel_volume": fuel_volume,
                            "transaction_amount": transaction_amount,
                            "flow_rate": flow_rate,
                            "start_timestamp": start_timestamp,
                            "end_timestamp": end_timestamp,
                            "dispenser_timestamp": dispenser_timestamp,
                            "device_timestamp": device_timestamp,
                            "receive_timestamp": receive_timestamp,
                            "create_timestamp": datetime.datetime.utcnow(),
                            "item_type": self.item_type_Transaction_Details,
                            "reference_number": str(grade_number),
                            "target_table": self.target_table_Transaction_Details,
                            "target_data_id": None,
                            "item_status": self.code_item_status_success,
                            "status_message": None
                        }
                    else:
                        message_item = {
                            "item_type": self.item_type_Transaction_Details,
                            "reference_number": str(grade_number),
                            "target_table": self.target_table_Transaction_Details,
                            "target_data_id": None,
                            "item_status": self.code_item_status_failure,
                            "status_message": ("\n".join(failure_array)),
                            "create_timestamp": datetime.datetime.utcnow()
                        }
                        
                    print("data_transaction", message_item)    
                        
                    if (message_item is not None):
                        message_data_success_count = 0
                        message_data_failure_count = 0
                        message_data_duplicate_count = 0
                        
                        if (message_item.get("item_status") == self.code_item_status_success):
                            
                            # Check if data already exists
                            cursor = self.db_con.cursor()
                            sql_query = """SELECT count(*) as record_count FROM data_transaction WHERE dispenser_id = %s AND device_timestamp = %s """
                            check_tuple = (message_item.get("dispenser_id"),  message_item.get("device_timestamp"))
                            cursor.execute(sql_query, check_tuple)
                            record_count = cursor.fetchone()[
                                'record_count']
                            print(record_count)
                                
                            if (record_count == 0):
                                
                                #Insert into main table (data_transaction)
                                self.db_con.begin()
                                cursor = self.db_con.cursor()
                                sql_query = """INSERT INTO data_transaction ( dispenser_id, pump_number, grade_number, price_type, price_value, fuel_volume, transaction_amount, flow_rate, start_timestamp, end_timestamp, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                insert_tuple = (message_item.get("dispenser_id"), message_item.get("pump_number"), message_item.get("grade_number"), message_item.get("price_type"), message_item.get("price_value"), message_item.get("fuel_volume"), message_item.get("transaction_amount"), message_item.get("flow_rate"), message_item.get("start_timestamp"),  message_item.get("end_timestamp"), message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                cursor.execute(
                                    sql_query, insert_tuple)
                                data_transaction_id = cursor.lastrowid
                                cursor.close()
                                self.db_con.commit()
            
                                message_data_success_count = message_data_success_count + 1
                                    
            
                            else:
                                message_data_duplicate_count = message_data_duplicate_count + 1
            
                        else:
                            
                            message_data_failure_count = message_data_failure_count + 1
                    else:
                        pass
                        
                    logger.info(
                        "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")

                
            elif event["A"].split('/')[1] == "pu" and event["A"].split('/')[2] == "st":
                if 'A' in event:
                    # print("imei: " +event["A"].split('/')[0])
                    imei = event["A"].split('/')[0]
                    dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
                    
                    create_timestamp = None
                    receive_timestamp = None
                    dispenser_timestamp = None
                    device_timestamp = None
                    pump_number = None
                    status_desc = None
                    event_code = None
                    event_name = 'pump'
                    
                    if 'B' in event:
                        unix_timestamp = event['B']
                        device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    
                    if 'Pu' in event:
                        pump_number = event['Pu']
                    
                    if 'St' in event:
                        if event['St'] is not None:
                            status_desc = event['St']
                        
                        
                    if status_desc == "busy" or status_desc == "authorized" or status_desc == "calling":
                        event_code = 'B'
                    elif status_desc == "idle" or status_desc == "FEOT  end tran" or status_desc == "PEOT  end tran":
                        event_code = 'I'
                    
                        
                    failure_array  = []    
                    
                    if (device_timestamp is None):
                        failure_array.append(
                            "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                    else:
                        input_date = str(device_timestamp)
                        input_date = input_date.split(' ')
                        year, month, day = input_date[0].split('-')
                        isValid = True
                        try:
                            datetime.datetime(int(year),int(month),int(day))
                        except:
                            isValid = False  
                        if (isValid == False):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]") 
            
                    if(pump_number is not None):
                        pump_number = int(pump_number)
                        
                    # if(event_code is not None):
                    #     event_code = str(event_code)
            
                    # if(status_desc is not None):
                    #     status_desc =  str(status_desc)
                 
                    if (len(failure_array) == 0):
                        message_item = {
                            "dispenser_id": dispenser_id,
                            "pump_number": pump_number,
                            "event_name": event_name,
                            "event_code": event_code,
                            "status_desc": status_desc,
                            "dispenser_timestamp": dispenser_timestamp,
                            "device_timestamp": device_timestamp,
                            "receive_timestamp": receive_timestamp,
                            "create_timestamp": datetime.datetime.utcnow(),
                            "target_data_id": None,
                            "item_status": self.code_item_status_success,
                            "status_message": None
                        }
                    else:
                        message_item = {
                            "target_table": self.target_table_event_pump,
                            "target_data_id": None,
                            "item_status": self.code_item_status_failure,
                            "status_message": ("\n".join(failure_array)),
                            "create_timestamp": datetime.datetime.utcnow()
                        }
                        
                    if (message_item is not None):
                        message_data_success_count = 0
                        message_data_failure_count = 0
                        message_data_duplicate_count = 0
                        
                        if (message_item.get("item_status") == self.code_item_status_success):
                            
                            # Check if data already exists
                            cursor = self.db_con.cursor()
                            sql_query = """SELECT count(*) as record_count FROM event_pump WHERE dispenser_id = %s AND event_name = %s AND device_timestamp = %s """
                            check_tuple = (message_item.get("dispenser_id"), message_item.get("event_name"), message_item.get("device_timestamp"))
                            cursor.execute(sql_query, check_tuple)
                            record_count = cursor.fetchone()[
                                'record_count']
                                
                            if (record_count == 0):
                                
                                #Insert into main table (event_pump)
                                self.db_con.begin()
                                cursor = self.db_con.cursor()
                                sql_query = """INSERT INTO event_pump ( dispenser_id, pump_number, event_name, event_code, status_desc, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                insert_tuple = (message_item.get("dispenser_id"), message_item.get("pump_number"), message_item.get("event_name"), message_item.get("event_code"), message_item.get("status_desc"), message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                cursor.execute(
                                    sql_query, insert_tuple)
                                event_pump_id = cursor.lastrowid
                                cursor.close()
                                self.db_con.commit()
            
                                message_data_success_count = message_data_success_count + 1
                                    
            
                            else:
                                message_data_duplicate_count = message_data_duplicate_count + 1
            
                        else:
                            
                            message_data_failure_count = message_data_failure_count + 1
                    else:
                        pass
                        
                    logger.info(
                        "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")
                        
            elif event["A"].split('/')[1] == "pu" and event["A"].split('/')[2] == "pp":
                try:
                    if 'A' in event:
                        imei = event["A"].split('/')[0]
                        dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
                        
                        create_timestamp = None
                        receive_timestamp = None
                        dispenser_timestamp = None
                        device_timestamp = None
                        grade_number = None
                        price_type = None
                        price_value = None
                        
                        if 'B' in event:
                            unix_timestamp = event['B']
                            device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        
                        if 'Pl' in event:
                            price_type = event['Pl']
                        
                        if 'Gr' in event:
                            grade_number = event['Gr']
                            
                        if 'Up' in event:
                            price_value = event['Up']
                            
                        if price_type == "Level 1":
                            price_type = 1
                        else:
                            price_type = 2
                                
                        failure_array  = []         
                        
                        if (device_timestamp is None):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                        else:
                            input_date = str(device_timestamp)
                            input_date = input_date.split(' ')
                            year, month, day = input_date[0].split('-')
                            isValid = True
                            try:
                                datetime.datetime(int(year),int(month),int(day))
                            except:
                                isValid = False  
                            if (isValid == False):
                                failure_array.append(
                                    "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]") 
                
                        if(price_type is not None):
                            price_type = int(price_type)
                            
                        if(grade_number is not None):
                            grade_number =  int(grade_number)
                            
                        if(price_value is not None):
                            price_value = float(price_value)           
                        
                        
                        if (len(failure_array) == 0):
                            message_item = {
                                "dispenser_id": dispenser_id,
                                "grade_number": grade_number,
                                "price_type": price_type,
                                "price_value": price_value,
                                "dispenser_timestamp": dispenser_timestamp,
                                "device_timestamp": device_timestamp,
                                "receive_timestamp": receive_timestamp,
                                "create_timestamp": datetime.datetime.utcnow(),
                                "target_data_id": None,
                                "item_status": self.code_item_status_success,
                                "status_message": None
                            }
                        else:
                            message_item = {
                                "item_type": self.item_type_Price,
                                "reference_number": str(grade_number),
                                "target_table": self.target_table_Price,
                                "target_data_id": None,
                                "item_status": self.code_item_status_failure,
                                "status_message": ("\n".join(failure_array)),
                                "create_timestamp": datetime.utcnow()
                            }
                            
                        if (message_item is not None):
                            message_data_success_count = 0
                            message_data_failure_count = 0
                            message_data_duplicate_count = 0
                            
                            if (message_item.get("item_status") == self.code_item_status_success):
                                
                                # Check if data already exists
                                cursor = self.db_con.cursor()
                                sql_query = """SELECT count(*) as record_count FROM data_price WHERE dispenser_id = %s AND grade_number = %s AND price_type = %s AND price_value = %s AND device_timestamp = %s """
                                check_tuple = (message_item.get("dispenser_id"), message_item.get("grade_number"), message_item.get("price_type"), message_item.get("price_value"), message_item.get("device_timestamp"))
                                cursor.execute(sql_query, check_tuple)
                                record_count = cursor.fetchone()[
                                    'record_count']
                                    
                                if (record_count == 0):
                                    
                                    #Insert into main table (data_price)
                                    self.db_con.begin()
                                    cursor = self.db_con.cursor()
                                    sql_query = """INSERT INTO data_price ( dispenser_id, grade_number, price_type, price_value, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"""
                                    insert_tuple = (message_item.get("dispenser_id"), message_item.get("grade_number"), message_item.get("price_type"), message_item.get("price_value"), message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                    cursor.execute(
                                        sql_query, insert_tuple)
                                    data_price_id = cursor.lastrowid
                                    cursor.close()
                                    self.db_con.commit()
    
                                    message_data_success_count = message_data_success_count + 1
                                        
    
                                else:
                                    message_data_duplicate_count = message_data_duplicate_count + 1
    
                            else:
                                
                                message_data_failure_count = message_data_failure_count + 1
                        else:
                            pass
                            
                         
                        logger.info(
                            "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")
    
                except Exception as e:
                    raise Exception(e)            
                            
                        
            elif event["A"].split('/')[1] == "pu" and event["A"].split('/')[2] == "co":
                try:
                    if 'A' in event:
                        imei = event["A"].split('/')[0]
                        dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
                        
                        create_timestamp = None
                        receive_timestamp = None
                        dispenser_timestamp = None
                        device_timestamp = None
                        pump_number = None
                        status_desc = None
                        event_code = None
                        event_name = 'pump'
                        
                        if 'B' in event:
                            unix_timestamp = event['B']
                            device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        
                        if 'Pu' in event:
                            pump_number = event['Pu']
                            
                        # if pump_number == None:
                        #     cursor = self.db_con.cursor()
                        #     sql_query = (""" SELECT side_a_number, side_b_number FROM dispenser WHERE dispenser_id = %s """ %(dispenser_id))
                        #     cursor.execute(sql_query)
                        #     pump_result = cursor.fetchone()
                            
                        #     if pump_result is not None:
                        #         side_a_number = pump_result['side_a_number']
                        #         side_b_number = pump_result
                                
                        # pump_number = [side_a_number, side_b_number]     
                            
                        if 'De' in event:
                            status_desc = event['De']
                        
                        if 'St' in event:
                            if event['St'] == 'up':
                                event_code = 'U'
                            else:
                                event['St'] == 'dn'
                                event_code = 'D'
                                
                        failure_array  = []         
                        
                        if (device_timestamp is None):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                        else:
                            input_date = str(device_timestamp)
                            input_date = input_date.split(' ')
                            year, month, day = input_date[0].split('-')
                            isValid = True
                            try:
                                datetime.datetime(int(year),int(month),int(day))
                            except:
                                isValid = False  
                            if (isValid == False):
                                failure_array.append(
                                    "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]") 
                
                        if(pump_number is not None):
                            pump_number = int(pump_number)
                        
                        if(event_code is not None):
                            event_code = str(event_code)
    
                        if(status_desc is not None):
                            status_desc =  str(status_desc)
                     
                        if (len(failure_array) == 0):
                            message_item = {
                                "dispenser_id": dispenser_id,
                                "pump_number": pump_number,
                                "event_name": event_name,
                                "event_code": event_code,
                                "status_desc": status_desc,
                                "dispenser_timestamp": dispenser_timestamp,
                                "device_timestamp": device_timestamp,
                                "receive_timestamp": receive_timestamp,
                                "create_timestamp": datetime.datetime.utcnow(),
                                "target_data_id": None,
                                "item_status": self.code_item_status_success,
                                "status_message": None
                            }
                        else:
                            message_item = {
                                "target_table": self.target_table_event_pump,
                                "target_data_id": None,
                                "item_status": self.code_item_status_failure,
                                "status_message": ("\n".join(failure_array)),
                                "create_timestamp": datetime.datetime.utcnow()
                            }
                            
                        if (message_item is not None):
                            message_data_success_count = 0
                            message_data_failure_count = 0
                            message_data_duplicate_count = 0
                            
                            if (message_item.get("item_status") == self.code_item_status_success):
                                
                                # Check if data already exists
                                cursor = self.db_con.cursor()
                                sql_query = """SELECT count(*) as record_count FROM event_pump WHERE dispenser_id = %s AND event_name = %s AND device_timestamp = %s AND pump_number = %s AND event_code = %s"""
                                check_tuple = (message_item.get("dispenser_id"),message_item.get("event_name"), message_item.get("device_timestamp"), message_item.get("pump_number"), message_item.get("event_code"))
                                cursor.execute(sql_query, check_tuple)
                                record_count = cursor.fetchone()[
                                    'record_count']
                                print(record_count)    
                                if (record_count == 0):
                                    
                                    #Insert into main table (data_price)
                                    self.db_con.begin()
                                    cursor = self.db_con.cursor()
                                    sql_query = """INSERT INTO event_pump ( dispenser_id, pump_number, event_name, event_code, status_desc, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                    insert_tuple = (message_item.get("dispenser_id"), pump_number, message_item.get("event_name"), message_item.get("event_code"), message_item.get("status_desc"), message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                    cursor.execute(
                                        sql_query, insert_tuple)
                                    event_pump_id = cursor.lastrowid
                                    cursor.close()
                                    self.db_con.commit()
    
                                    message_data_success_count = message_data_success_count + 1
                                        
    
                                else:
                                    message_data_duplicate_count = message_data_duplicate_count + 1
    
                            else:
                                
                                message_data_failure_count = message_data_failure_count + 1
                        else:
                            pass
                            
                        logger.info(
                            "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")
                
                except Exception as e:
                    raise Exception(e)    
                    
            elif event["A"].split('/')[1] == "py" and event["A"].split('/')[2] == "st":
                try:
                    if 'A' in event:
                        imei = event["A"].split('/')[0]
                        dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
                        
                        create_timestamp = None
                        receive_timestamp = None
                        dispenser_timestamp = None
                        device_timestamp = None
                        side_label = None
                        ip_address = None
                        component_name = None
                        component_status = None
                        extended_status = None
                        error_info = None
                        
                        if 'B' in event:
                            unix_timestamp = event['B']
                            device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        
                        if 'Ip' in event:
                            ip_address = event['Ip']
                            
                        if 'Sd' in event:
                            side_label = event['Sd']
                            
                        if 'De' in event:
                            component_name = event['De']
                            
                        if 'St' in event:
                            component_status = event['St']
                            
                        if 'Sx' in event:
                            extended_status = event['Sx']
                            
                        if 'Er' in event:
                            error_info = event['Er']    
                                
                        failure_array  = []         
                        
                        if (device_timestamp is None):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                        else:
                            input_date = str(device_timestamp)
                            input_date = input_date.split(' ')
                            year, month, day = input_date[0].split('-')
                            isValid = True
                            try:
                                datetime.datetime(int(year),int(month),int(day))
                            except:
                                isValid = False  
                            if (isValid == False):
                                failure_array.append(
                                    "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]") 
                
                        if(ip_address is not None):
                            ip_address = str(ip_address)
                        
                        if(side_label is not None):
                            side_label = str(side_label)
    
                        if(component_name is not None):
                            component_name =  str(component_name)
                            
                        if(component_status is not None):
                            component_status =  str(component_status)
                            
                        if(extended_status is not None):
                            extended_status =  str(extended_status)
                            
                        if(error_info is not None):
                            error_info =  str(error_info)
                     
                        if (len(failure_array) == 0):
                            message_item = {
                                "dispenser_id": dispenser_id,
                                "ip_address": ip_address,
                                "side_label": side_label,
                                "component_name": component_name,
                                "component_status": component_status,
                                "extended_status": extended_status,
                                "error_info": error_info,
                                "dispenser_timestamp": dispenser_timestamp,
                                "device_timestamp": device_timestamp,
                                "receive_timestamp": receive_timestamp,
                                "create_timestamp": datetime.datetime.utcnow(),
                                "target_data_id": None,
                                "item_status": self.code_item_status_success,
                                "status_message": None
                            }
                        else:
                            message_item = {
                                "target_table": self.target_table_crind_status,
                                "target_data_id": None,
                                "item_status": self.code_item_status_failure,
                                "status_message": ("\n".join(failure_array)),
                                "create_timestamp": datetime.datetime.utcnow()
                            }
                            
                        if (message_item is not None):
                            message_data_success_count = 0
                            message_data_failure_count = 0
                            message_data_duplicate_count = 0
                            
                            if (message_item.get("item_status") == self.code_item_status_success):
                                
                                # Check if data already exists
                                cursor = self.db_con.cursor()
                                sql_query = """SELECT count(*) as record_count FROM crind_status WHERE dispenser_id = %s AND device_timestamp = %s AND side_label = %s AND component_name = %s AND component_status = %s AND extended_status = %s AND error_info = %s"""
                                check_tuple = (message_item.get("dispenser_id"), message_item.get("device_timestamp"), message_item.get("side_label"), message_item.get("component_name"), message_item.get("component_status"), message_item.get("extended_status"), message_item.get("error_info"))
                                cursor.execute(sql_query, check_tuple)
                                record_count = cursor.fetchone()[
                                    'record_count']
                                print(record_count)    
                                if (record_count == 0):
                                    
                                    #Insert into main table (crind_status)
                                    self.db_con.begin()
                                    cursor = self.db_con.cursor()
                                    sql_query = """INSERT INTO crind_status ( dispenser_id, ip_address, side_label, component_name, component_status, extended_status, error_info, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                    insert_tuple = (message_item.get("dispenser_id"), message_item.get("ip_address"), message_item.get("side_label"), message_item.get("component_name"), message_item.get("component_status"), message_item.get("extended_status"), message_item.get("error_info"), message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                    cursor.execute(
                                        sql_query, insert_tuple)
                                    event_pump_id = cursor.lastrowid
                                    cursor.close()
                                    self.db_con.commit()
    
                                    message_data_success_count = message_data_success_count + 1
                                    
    
                                else:
                                    message_data_duplicate_count = message_data_duplicate_count + 1
    
                            else:
                                
                                message_data_failure_count = message_data_failure_count + 1
                        else:
                            pass
                            
                        logger.info(
                            "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")
                
                except Exception as e:
                    raise Exception(e)

            elif event["A"].split('/')[1] == "py" and event["A"].split('/')[2] == "hw":
                try:
                    if 'A' in event:
                        imei = event["A"].split('/')[0]
                        dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
                        
                        create_timestamp = None
                        receive_timestamp = None
                        dispenser_timestamp = None
                        device_timestamp = None
                        side = None
                        hardware = None
                        value = None
                        
                        if 'B' in event:
                            unix_timestamp = event['B']
                            device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        
                        if 'Sd' in event:
                            side = event['Sd']
                            
                        if 'Hw' in event:
                            hardware = event['Hw']
                            
                        if 'Va' in event:
                            value = event['Va']
                                
                        failure_array  = []         
                        
                        if (device_timestamp is None):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                        else:
                            input_date = str(device_timestamp)
                            input_date = input_date.split(' ')
                            year, month, day = input_date[0].split('-')
                            isValid = True
                            try:
                                datetime.datetime(int(year),int(month),int(day))
                            except:
                                isValid = False  
                            if (isValid == False):
                                failure_array.append(
                                    "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]")
                        
                        if(side is not None):
                            side = str(side)
    
                        if(hardware is not None):
                            hardware =  str(hardware)
                            
                        if(value is not None):
                            value =  str(value)
                     
                        if (len(failure_array) == 0):
                            message_item = {
                                "dispenser_id": dispenser_id,
                                "side": side,
                                "hardware": hardware,
                                "value": value,
                                "dispenser_timestamp": dispenser_timestamp,
                                "device_timestamp": device_timestamp,
                                "receive_timestamp": receive_timestamp,
                                "create_timestamp": datetime.datetime.utcnow(),
                                "target_data_id": None,
                                "item_status": self.code_item_status_success,
                                "status_message": None
                            }
                        else:
                            message_item = {
                                "target_table": self.target_table_crind_hardware,
                                "target_data_id": None,
                                "item_status": self.code_item_status_failure,
                                "status_message": ("\n".join(failure_array)),
                                "create_timestamp": datetime.datetime.utcnow()
                            }
                            
                        if (message_item is not None):
                            message_data_success_count = 0
                            message_data_failure_count = 0
                            message_data_duplicate_count = 0
                            
                            if (message_item.get("item_status") == self.code_item_status_success):
                                
                                # Check if data already exists
                                cursor = self.db_con.cursor()
                                sql_query = """SELECT count(*) as record_count FROM crind_hardware WHERE dispenser_id = %s AND device_timestamp = %s AND side = %s AND hardware = %s"""
                                check_tuple = (message_item.get("dispenser_id"), message_item.get("device_timestamp"), message_item.get("side"), message_item.get("hardware"))
                                cursor.execute(sql_query, check_tuple)
                                record_count = cursor.fetchone()[
                                    'record_count']
                                print(record_count)    
                                if (record_count == 0):
                                    
                                    #Insert into main table (crind_hardware)
                                    self.db_con.begin()
                                    cursor = self.db_con.cursor()
                                    sql_query = """INSERT INTO crind_hardware ( dispenser_id, side, hardware, value, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"""
                                    insert_tuple = (message_item.get("dispenser_id"), message_item.get("side"), message_item.get("hardware"), message_item.get("value"), message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                    cursor.execute(
                                        sql_query, insert_tuple)
                                    event_pump_id = cursor.lastrowid
                                    cursor.close()
                                    self.db_con.commit()
    
                                    message_data_success_count = message_data_success_count + 1
                                        
    
                                else:
                                    message_data_duplicate_count = message_data_duplicate_count + 1
    
                            else:
                                
                                message_data_failure_count = message_data_failure_count + 1
                        else:
                            pass
                            
                        logger.info(
                            "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")
                
                except Exception as e:
                    raise Exception(e)
                    
            elif event["A"].split('/')[1] == "py" and event["A"].split('/')[2] == "sw":
                try:
                    if 'A' in event:
                        imei = event["A"].split('/')[0]
                        dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
                        
                        create_timestamp = None
                        receive_timestamp = None
                        dispenser_timestamp = None
                        device_timestamp = None
                        side = None
                        software = None
                        value = None
                        type = None
                        
                        if 'B' in event:
                            unix_timestamp = event['B']
                            device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        
                        if 'Sd' in event:
                            side = event['Sd']
                            
                        if 'Sw' in event:
                            software = event['Sw']
                            
                        if 'Va' in event:
                            value = event['Va']
                        
                        if 'Ty' in event:
                            type = event['Ty']    
                                
                        failure_array  = []      
                        
                        if (device_timestamp is None):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                        else:
                            input_date = str(device_timestamp)
                            input_date = input_date.split(' ')
                            year, month, day = input_date[0].split('-')
                            isValid = True
                            try:
                                datetime.datetime(int(year),int(month),int(day))
                            except:
                                isValid = False  
                            if (isValid == False):
                                failure_array.append(
                                    "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]")
                        
                        if(side is not None):
                            side = str(side)
    
                        if(software is not None):
                            software =  str(software)
                            
                        if(value is not None):
                            value =  str(value)
                        
                        if(type is not None):
                            type = int(type)
                            
                        sw = []
                        if software == 'PCI_Bui':
                            value = json.loads(value)
                            if value is not None and len(value) > 0:
                                for row_event in value:
                                    if row_event['Mo'] == '[BOOT]':
                                        boot_version = row_event['Ve']
                                        name = 'BOOT'
                                        data = {"software": name, "value":boot_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[OS]':
                                        software_version = row_event['Ve']
                                        name = 'OS'
                                        data = {"software": name, "value":software_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[CRINDBIOS]':
                                        crind_bios_version = row_event['Ve']
                                        name = 'CRINDBIOS'
                                        data = {"software": name, "value":crind_bios_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[DISPLAYMANAGER]':
                                        display_version = row_event['Ve']
                                        name = 'DISPLAYMANAGER'
                                        data = {"software": name, "value":display_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[EMV]':
                                        emv_version = row_event['Ve']
                                        name = 'EMV'
                                        data = {"software": name, "value":emv_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[CLOUD]':
                                        cloud_version = row_event['Ve']
                                        name = 'CLOUD'
                                        data = {"software": name, "value":cloud_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[DIAGNOSTIC]':
                                        diagnostic_version = row_event['Ve']
                                        name = 'DIAGNOSTIC'
                                        data = {"software": name, "value":diagnostic_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[OPT]':
                                        opt_version = row_event['Ve']
                                        name = 'OPT'
                                        data = {"software": name, "value":opt_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[RESOURCES]':
                                        resources_version = row_event['Ve']
                                        name = 'RESOURSES'
                                        data = {"software": name, "value":resources_version}
                                        sw.append(data)
                                    if row_event['Mo'] == '[VFI]':
                                        vfi_version = row_event['Ve']
                                        name = 'VFI'
                                        data = {"software": name, "value":vfi_version}
                                        sw.append(data)
                        else:
                            sw = value
                     
                        if (len(failure_array) == 0):
                            message_item = {
                                "dispenser_id": dispenser_id,
                                "side": side,
                                "software": software,
                                "type" : type,
                                "value": sw,
                                "dispenser_timestamp": dispenser_timestamp,
                                "device_timestamp": device_timestamp,
                                "receive_timestamp": receive_timestamp,
                                "create_timestamp": datetime.datetime.utcnow(),
                                "target_data_id": None,
                                "item_status": self.code_item_status_success,
                                "status_message": None
                            }
                        else:
                            message_item = {
                                "target_table": self.target_table_crind_software,
                                "target_data_id": None,
                                "item_status": self.code_item_status_failure,
                                "status_message": ("\n".join(failure_array)),
                                "create_timestamp": datetime.datetime.utcnow()
                            }
                            
                        if (message_item is not None):
                            message_data_success_count = 0
                            message_data_failure_count = 0
                            message_data_duplicate_count = 0
                            
                            if (message_item.get("item_status") == self.code_item_status_success):
                                
                                if message_item.get("software") == "PCI_Bui":
                                    pci_bui_data = message_item.get("value")
                                    
                                    if pci_bui_data is not None and len(pci_bui_data) > 0:
                                        for row in pci_bui_data:
                                            software = row['software']
                                            value = row['value']
                                    
                                            # Check if data already exists
                                            cursor = self.db_con.cursor()
                                            sql_query = """SELECT count(*) as record_count FROM crind_software WHERE dispenser_id = %s AND device_timestamp = %s AND side = %s AND software = %s AND type = %s AND value = %s """
                                            check_tuple = (message_item.get("dispenser_id"), message_item.get("device_timestamp"), message_item.get("side"), software, message_item.get("type"), value)
                                            cursor.execute(sql_query, check_tuple)
                                            record_count = cursor.fetchone()[
                                                'record_count']
                                                
                                            if (record_count == 0):
                                                
                                                #Insert into main table (crind_software)
                                                self.db_con.begin()
                                                cursor = self.db_con.cursor()
                                                sql_query = """INSERT INTO crind_software ( dispenser_id, side, software, type, value, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                                insert_tuple = (message_item.get("dispenser_id"), message_item.get("side"), software, message_item.get("type"), value, message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                                cursor.execute(
                                                    sql_query, insert_tuple)
                                                event_pump_id = cursor.lastrowid
                                                cursor.close()
                                                self.db_con.commit()
                
                                                message_data_success_count = message_data_success_count + 1
                                                    
                
                                            else:
                                                message_data_duplicate_count = message_data_duplicate_count + 1
                                                
                                else:
                                    software = message_item.get("software")
                                    value = message_item.get("value")
                                    
                                    # Check if data already exists
                                    cursor = self.db_con.cursor()
                                    sql_query = """SELECT count(*) as record_count FROM crind_software WHERE dispenser_id = %s AND device_timestamp = %s AND side = %s AND software = %s AND type = %s AND value = %s """
                                    check_tuple = (message_item.get("dispenser_id"), message_item.get("device_timestamp"), message_item.get("side"), software, message_item.get("type"), value)
                                    cursor.execute(sql_query, check_tuple)
                                    record_count = cursor.fetchone()[
                                        'record_count']
                                        
                                    if (record_count == 0):
                                        
                                        #Insert into main table (crind_software)
                                        self.db_con.begin()
                                        cursor = self.db_con.cursor()
                                        sql_query = """INSERT INTO crind_software ( dispenser_id, side, software, type, value, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        insert_tuple = (message_item.get("dispenser_id"), message_item.get("side"), software, message_item.get("type"), value, message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                        cursor.execute(
                                            sql_query, insert_tuple)
                                        event_pump_id = cursor.lastrowid
                                        cursor.close()
                                        self.db_con.commit()
        
                                        message_data_success_count = message_data_success_count + 1
                                            
        
                                    else:
                                        message_data_duplicate_count = message_data_duplicate_count + 1
    
                            else:
                                
                                message_data_failure_count = message_data_failure_count + 1
                        else:
                            pass
                            
                        logger.info(
                            "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")
                
                except Exception as e:
                    raise Exception(e)                    
                    
            elif event["A"].split('/')[1] == "py" and event["A"].split('/')[2] == "dx":
                try:
                    if 'A' in event:
                        imei = event["A"].split('/')[0]
                        dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
                        
                        create_timestamp = None
                        receive_timestamp = None
                        dispenser_timestamp = None
                        device_timestamp = None
                        device = None
                        bytes = None
                        messages = None
                        
                        if 'B' in event:
                            unix_timestamp = event['B']
                            device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        
                        if 'De' in event:
                            device = event['De']
                            
                        if 'nB' in event:
                            bytes = event['nB']
                            
                        if 'nM' in event:
                            messages = event['nM']
                                
                        failure_array  = []      
                        
                        if (device_timestamp is None):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                        else:
                            input_date = str(device_timestamp)
                            input_date = input_date.split(' ')
                            year, month, day = input_date[0].split('-')
                            isValid = True
                            try:
                                datetime.datetime(int(year),int(month),int(day))
                            except:
                                isValid = False  
                            if (isValid == False):
                                failure_array.append(
                                    "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]")
                        
                        if(device is not None):
                            device = str(device)
    
                        if(bytes is not None):
                            bytes =  str(bytes)
                            
                        if(messages is not None):
                            messages =  str(messages)
                     
                        if (len(failure_array) == 0):
                            message_item = {
                                "dispenser_id": dispenser_id,
                                "device": device,
                                "bytes": bytes,
                                "messages": messages,
                                "dispenser_timestamp": dispenser_timestamp,
                                "device_timestamp": device_timestamp,
                                "receive_timestamp": receive_timestamp,
                                "create_timestamp": datetime.datetime.utcnow(),
                                "target_data_id": None,
                                "item_status": self.code_item_status_success,
                                "status_message": None
                            }
                        else:
                            message_item = {
                                "target_table": self.target_table_crind_software,
                                "target_data_id": None,
                                "item_status": self.code_item_status_failure,
                                "status_message": ("\n".join(failure_array)),
                                "create_timestamp": datetime.datetime.utcnow()
                            }
                            
                        if (message_item is not None):
                            message_data_success_count = 0
                            message_data_failure_count = 0
                            message_data_duplicate_count = 0
                            
                            if (message_item.get("item_status") == self.code_item_status_success):
                                
                                # Check if data already exists
                                cursor = self.db_con.cursor()
                                sql_query = """SELECT count(*) as record_count FROM data_transmission_statistics WHERE dispenser_id = %s AND device_timestamp = %s AND device = %s"""
                                check_tuple = (message_item.get("dispenser_id"), message_item.get("device_timestamp"), message_item.get("device"))
                                cursor.execute(sql_query, check_tuple)
                                record_count = cursor.fetchone()[
                                    'record_count']
                                print(record_count)    
                                if (record_count == 0):
                                    
                                    #Insert into main table (data_transmission_statistics)
                                    self.db_con.begin()
                                    cursor = self.db_con.cursor()
                                    sql_query = """INSERT INTO data_transmission_statistics ( dispenser_id, device, bytes, messages, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"""
                                    insert_tuple = (message_item.get("dispenser_id"), message_item.get("device"), message_item.get("bytes"), message_item.get("messages"), message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                    cursor.execute(
                                        sql_query, insert_tuple)
                                    event_pump_id = cursor.lastrowid
                                    cursor.close()
                                    self.db_con.commit()
    
                                    message_data_success_count = message_data_success_count + 1
                                        
    
                                else:
                                    message_data_duplicate_count = message_data_duplicate_count + 1
    
                            else:
                                
                                message_data_failure_count = message_data_failure_count + 1
                        else:
                            pass
                            
                        logger.info(
                            "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")
                
                except Exception as e:
                    raise Exception(e)
            
            elif event["A"].split('/')[1] == "zm" and event["A"].split('/')[2] == "zs":
                if 'A' in event:
                    
                    imei = event["A"].split('/')[0]
                    dispenser_id,site_timezone = siteiq_ram_iot.get_dispenser_data(self.db_con, imei)
                    
                    create_timestamp = None
                    receive_timestamp = None
                    dispenser_timestamp = None
                    device_timestamp = None
                    pump_number = None
                    status_desc = None
                    event_code = None
                    event_name = 'serial interface'
                    
                    if 'B' in event:
                        unix_timestamp = event['B']
                        device_timestamp = datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    
                    if 'St' in event:
                        if event['St'] is not None:
                            event_code = event['St']
                        
                    if event_code is not None:
                        
                        if event_code.lower() == "ok":
                            status_desc = 'Connected'
                            
                        elif event_code.lower() == "ko":
                            status_desc = 'Disconnected'
                    
                        
                    failure_array  = []    
                    
                    if (device_timestamp is None):
                        failure_array.append(
                            "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is blank]")
                    else:
                        input_date = str(device_timestamp)
                        input_date = input_date.split(' ')
                        year, month, day = input_date[0].split('-')
                        isValid = True
                        try:
                            datetime.datetime(int(year),int(month),int(day))
                        except:
                            isValid = False  
                        if (isValid == False):
                            failure_array.append(
                                "Value: " + str(device_timestamp) + " [Issue: Device Timestamp is not valid]")
                 
                    if (len(failure_array) == 0):
                        message_item = {
                            "dispenser_id": dispenser_id,
                            "pump_number": pump_number,
                            "event_name": event_name,
                            "event_code": event_code,
                            "status_desc": status_desc,
                            "dispenser_timestamp": dispenser_timestamp,
                            "device_timestamp": device_timestamp,
                            "receive_timestamp": receive_timestamp,
                            "create_timestamp": datetime.datetime.utcnow(),
                            "target_data_id": None,
                            "item_status": self.code_item_status_success,
                            "status_message": None
                        }
                    else:
                        message_item = {
                            "target_table": self.target_table_event_pump,
                            "target_data_id": None,
                            "item_status": self.code_item_status_failure,
                            "status_message": ("\n".join(failure_array)),
                            "create_timestamp": datetime.datetime.utcnow()
                        }
                        
                    if (message_item is not None):
                        message_data_success_count = 0
                        message_data_failure_count = 0
                        message_data_duplicate_count = 0
                        
                        if (message_item.get("item_status") == self.code_item_status_success):
                            
                            # Check if data already exists
                            cursor = self.db_con.cursor()
                            sql_query = """SELECT count(*) as record_count FROM event_pump WHERE dispenser_id = %s AND event_name = %s AND device_timestamp = %s """
                            check_tuple = (message_item.get("dispenser_id"), message_item.get("event_name"), message_item.get("device_timestamp"))
                            cursor.execute(sql_query, check_tuple)
                            record_count = cursor.fetchone()[
                                'record_count']
                                
                            if (record_count == 0):
                                
                                #Insert into main table (data_price)
                                self.db_con.begin()
                                cursor = self.db_con.cursor()
                                sql_query = """INSERT INTO event_pump ( dispenser_id, pump_number, event_name, event_code, status_desc, dispenser_timestamp, device_timestamp, receive_timestamp, create_timestamp ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                insert_tuple = (message_item.get("dispenser_id"), message_item.get("pump_number"), message_item.get("event_name"), message_item.get("event_code"), message_item.get("status_desc"), message_item.get("dispenser_timestamp"), message_item.get("device_timestamp"), message_item.get("receive_timestamp"), message_item.get("create_timestamp"))
                                cursor.execute(
                                    sql_query, insert_tuple)
                                event_pump_id = cursor.lastrowid
                                cursor.close()
                                self.db_con.commit()
            
                                message_data_success_count = message_data_success_count + 1
                                    
            
                            else:
                                message_data_duplicate_count = message_data_duplicate_count + 1
            
                        else:
                            
                            message_data_failure_count = message_data_failure_count + 1
                    else:
                        pass
                        
                    logger.info(
                        "INFO: ThingTopic [siq_iot2_event_test] handle_topic() - Topic Message processing completed")
                    
        self.close_db_connection()
        logger.info("INFO: ThingTopic [siq_iot2_event_test] handle_topic() - END")
        
def lambda_handler(event, context):
    thing_topic = ThingTopic()
    event = {'A': '273625829595496/py/st', 'B': 1636564845, 'Ty': '4', 'Sd': 'A', 'De': 257, 'St': 2, 'Sx': 0, 'Er': 0} 
    thing_topic.topic_info(event)
