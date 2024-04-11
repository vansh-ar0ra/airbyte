#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from typing import Any, Iterable, Mapping
import threading
import requests
import time

import pika
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
from pika.adapters.blocking_connection import BlockingConnection
from pika.spec import BasicProperties


_DEFAULT_PORT = 5672
_ROUTING_KEY = "info"
_EXCHANGE = "lamatic_exchange"
_INDEX_QUERY = """mutation($documentObj: JSON!, $webhookURL: String) {
  IndexData(documentObj: $documentObj, webhookURL: $webhookURL)
}"""


def create_connection(config: Mapping[str, Any]) -> BlockingConnection:
    host = config.get("host")
    port = config.get("port") or _DEFAULT_PORT
    username = config.get("username")
    password = config.get("password")
    virtual_host = config.get("virtual_host", "")
    ssl_enabled = config.get("ssl",False)
    amqp_protocol = "amqp"
    host_url = host
    if port:
        host_url = host + ":" + str(port)
    credentials = f"{username}:{password}@" if username and password else ""
    params = pika.URLParameters(f"{amqp_protocol}://{credentials}{host_url}/{virtual_host}")
    return BlockingConnection(params)


def get_nested_required_fields(k, required_fields):

    new_requried_fields= []

    for field in required_fields:
        if "." in field:
            split_field = field.split(".")
            if split_field[0] == k:
                field = ".".join(split_field[1:])

        new_requried_fields.append(field)

    return new_requried_fields


def map_data(data_mapping, body, required_fields):
    try:        
        final_obj = {}
        data_mapping = json.loads(data_mapping)
        
        for k,v in data_mapping.items():
            
            if isinstance(v, str): 
                if v in body.keys() and body[v]!=None:
                    final_obj[k] = body[v]
                elif "*" in required_fields or k in required_fields:
                    raise ValueError(f"Error: Mapping for required field {k}: {v} does not map to any field in the Source connector data")
                
            if isinstance(v, dict): 
                new_mapping = json.dumps(v)

                if "*" in required_fields: 
                    mapped_obj = map_data(new_mapping, body, ["*"])
                else: 
                    new_required_fields = get_nested_required_fields(k, required_fields)
                    mapped_obj = map_data(new_mapping, body, new_required_fields)

                if mapped_obj:
                    final_obj[k] = mapped_obj
        
        return final_obj
    
    except Exception as e:
        raise (f"Error in Data Mapping: {e}")


def consume_messages(config):
    # Establish a new connection and channel for each thread
    max_retries = 3

    def reconnect(connection):
        if connection:
            if connection.is_open:
                connection.close()

        connection = create_connection(config=config)
        return connection, connection.channel()
    
    connection, channel = reconnect(None)

    pod_URL = config.get('pod_URL')
    data_mapping = config.get("data_mapping","")
    bearer_token = config.get("bearer_token", "")
    required_fields = config.get("required_fields", "")
    
    # Ensure the queue exists
    # channel.queue_declare(queue=queue_name, durable=True)

    for attempt in range(max_retries):
        try: 
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            channel.queue_bind(
                exchange=_EXCHANGE, queue=queue_name, routing_key=_ROUTING_KEY)
            
        except pika.exceptions.StreamLostError as e:
            print(f"StreamLostError detected, attempting to reconnect, attempt {attempt + 1}")
            connection, channel = reconnect(connection)
            continue  # Continue to the next attempt

        except Exception as e:
            print(f"Unexpected error: {e}, attempting to reconnect, attempt {attempt + 1}")
            connection, channel = reconnect(connection)
            continue  # Continue to the next attempt
            
            # Set up a consumer
        for method_frame, properties, body in channel.consume(queue=queue_name, auto_ack=False, inactivity_timeout=60):
            if method_frame:
                try:
                    print(f" [x] Received {body.decode()}")
                    if (data_mapping): 
                        mapped_data = map_data(data_mapping, json.loads(body.decode()), required_fields)
                        print(f"Mapped Data: {mapped_data}")

                        variables = {"documentObj": mapped_data, "webhookURL": ""}
                        print(variables)
                        
                        if (bearer_token):
                            headers = {
                                "Authorization": f"Bearer {bearer_token}"
                            }

                            pod_response = requests.post(pod_URL, json={"query": _INDEX_QUERY, "variables": variables}, headers= headers)
                        else: 
                            pod_response = requests.post(pod_URL, json={"query": _INDEX_QUERY, "variables": variables})

                        print(pod_response)
                        print(pod_response.text)
                        print(" [x] Sent the Data to Pod")
                    
                    else:
                        print("No data mapping is provided")
                    # Acknowledge the message
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                except Exception as e:
                    print(f"Error in message processing: {e}")
            else:
                # Inactivity timeout reached, check if the thread should stop
                print("Stopping the Message Consumer for this Invocation of Write function")
                # Implement your logic here to decide whether to break the loop
                # For example, you can check a condition or wait for a signal
                break
        break
    
    # Cancel the consumer and close the connection when done
    channel.cancel()

    if connection.is_open:
        connection.close()


def start_consumer_thread(config):
    print("Consumer Thread Starting")
    consumer_thread = threading.Thread(target=consume_messages, args=(config,))
    consumer_thread.start()
    return consumer_thread


class DestinationLamatic(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        TODO
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        print("Executing Write function")

        max_retries = 3  # Max reconnection attempts

        consumer_thread = start_consumer_thread(config)
        time.sleep(1)

        def attempt_publish(connection, channel, message):
            try:
                headers = {"stream": message.record.stream, "emitted_at": message.record.emitted_at, "namespace": message.record.namespace}
                properties = BasicProperties(content_type="application/json", headers=headers)
                channel.basic_publish(
                    exchange=_EXCHANGE, routing_key=_ROUTING_KEY, properties=properties, body=json.dumps(message.record.data)
                )
                return True
            except pika.exceptions.AMQPError as e:
                print(f"Error publishing message: {e}")
                return False

        def reconnect(connection):
            if connection:
                if connection.is_open:
                    connection.close()

            connection = create_connection(config=config)
            return connection, connection.channel()
        
        connection, channel = reconnect(None)  # Initial connection setup

        streams = {s.stream.name for s in configured_catalog.streams}
        for message in input_messages:
            if message.type == Type.STATE:
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                if record.stream not in streams:
                    # Message contains record from a stream that is not in the catalog. Skip it!
                    continue

                success = attempt_publish(connection, channel, message)

                if not success:
                    print("Entering not success")
                    for attempt in range(max_retries):
                        print(f"Attempting to reconnect and retry, attempt {attempt + 1}")

                        consumer_thread.join()
                        consumer_thread = start_consumer_thread(config)
                        time.sleep(1)

                        connection, channel = reconnect(connection)
                        success = attempt_publish(connection, channel, message)
                        if success:
                            break
                        else:  # If all retries failed
                            print("Failed to publish message after multiple reconnection attempts.")
            else:
                # Let's ignore other message types for now
                continue

        consumer_thread.join()

        if connection.is_open:
            connection.close()


    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            connection = create_connection(config=config)
        except Exception as e:
            logger.error(f"Failed to create connection. Error: {e}")
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"Could not create connection: {repr(e)}")
        try:
            channel = connection.channel()
            if channel.is_open:
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            return AirbyteConnectionStatus(status=Status.FAILED, message="Could not open channel")
        except Exception as e:
            logger.error(f"Failed to open RabbitMQ channel. Error: {e}")
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
        finally:
            connection.close()
