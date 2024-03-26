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


def map_data(data_mapping, body):
    try: 
        final_obj = {}
        data_mapping = json.loads(data_mapping)
        
        for k,v in data_mapping.items():
            
            if isinstance(v, str): 
                if v in body.keys():
                    final_obj[k] = body[v]
                else: 
                    print(f"Error: Mapping for field {k}: {v} does not map to any field in the Source connector data")
                
            if isinstance(v, dict): 
                new_mapping = json.dumps(v)
                mapped_obj = map_data(new_mapping, body)
                if mapped_obj:
                    final_obj[k] = mapped_obj
        
        return final_obj
    
    except Exception as e:
        print(f"Error in Data Mapping: {e}")


def consume_messages(config):
    # Establish a new connection and channel for each thread
    host =  config.get('host')
    pod_URL = config.get('pod_URL')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host))
    channel = connection.channel()
    data_mapping = config.get("data_mapping","")
    bearer_token = config.get("bearer_token", "")
    
    # Ensure the queue exists
    # channel.queue_declare(queue=queue_name, durable=True)
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(
        exchange=_EXCHANGE, queue=queue_name, routing_key=_ROUTING_KEY)
    
    # Set up a consumer
    for method_frame, properties, body in channel.consume(queue=queue_name, auto_ack=False, inactivity_timeout=60):
        if method_frame:
            print(f" [x] Received {body.decode()}")

            if (data_mapping): 
                try:
                    mapped_data = map_data(data_mapping, json.loads(body.decode()))
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
                except Exception as e:
                    print("Exception occured in sending response to API", e)
            
            else:
                print("No data mapping is provided")

            # Acknowledge the message
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        else:
            # Inactivity timeout reached, check if the thread should stop
            print("Stopping the Message Consumer for this Invocation of Write function")
            # Implement your logic here to decide whether to break the loop
            # For example, you can check a condition or wait for a signal
            break
    
    # Cancel the consumer and close the connection when done
    channel.cancel()
    connection.close()


def start_consumer_thread(config):
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
        consumer_thread = start_consumer_thread(config)
        time.sleep(1)

        connection = create_connection(config=config)
        channel = connection.channel()

        streams = {s.stream.name for s in configured_catalog.streams}
        try:
            for message in input_messages:
                if message.type == Type.STATE:
                    # Emitting a state message means all records that came before it
                    # have already been published.
                    yield message
                elif message.type == Type.RECORD:
                    record = message.record
                    if record.stream not in streams:
                        # Message contains record from a stream that is not in the catalog. Skip it!
                        continue
                    headers = {"stream": record.stream, "emitted_at": record.emitted_at, "namespace": record.namespace}
                    properties = BasicProperties(content_type="application/json", headers=headers)
                    channel.basic_publish(
                        exchange=_EXCHANGE or "", routing_key=_ROUTING_KEY, properties=properties, body=json.dumps(record.data)
                    )
                else:
                    # Let's ignore other message types for now
                    continue
            consumer_thread.join()
        finally:
            connection.close()

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            # TODO

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
