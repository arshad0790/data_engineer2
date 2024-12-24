import json
import time
from confluent_kafka import Producer

class InvoiceProducer:
    def __init__(self):
        self.topic = "invoices"
        self.conf = {'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
                     'security.protocol': 'SASL_SSL',
                     'sasl.mechanism': 'PLAIN',
                     'sasl.username': 'WAMFMTSHUDP2D3I3',
                     'sasl.password': 'VXnP+yvvBC1F5nmO3J5pYEJ3GEwQj/YFE8GLDdmi9WloeaDhjo4AvW9Eoi6AEg2Y',
                     'client.id': "arshad-desktop"}
    
    def delivery_callback(self, err, msg):
        if err:
            print(f'ERROR: message delivery failed: {err}')
        else:
            key = msg.key().decode('utf-8')
            invoice_id = json.loads(msg.value().decode('utf-8'))["InvoiceNumber"]
            print(f"Produced event to: key = {key}  value = {invoice_id}")
        
    def produce_invoices(self, producer: Producer):
        with open("Apache-Spark-and-Databricks-Stream-Processing-in-Lakehouse-main/Kafka_Practice/data/invoices.json") as lines:
            for line in lines:
                invoice = json.loads(line)
                store_id = invoice['StoreID']
                producer.produce(self.topic, key=store_id, value=line, callback=self.delivery_callback)
                time.sleep(0.5)
                producer.poll(1)

    def start(self):
        kafka_producer = Producer(self.conf)
        self.produce_invoices(kafka_producer)
        kafka_producer.flush(10)

if __name__ == "__main__":
    invoice_producer = InvoiceProducer()
    invoice_producer.start()

