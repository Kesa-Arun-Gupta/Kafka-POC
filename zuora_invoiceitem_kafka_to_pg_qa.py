import io
import time
import json
import logging
import requests
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import inspect
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import config

# -------- Configuration --------
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC = config.KAFKA_TOPIC

PG_HOST = config.PG_HOST
PG_PORT = config.PG_PORT
PG_DB = config.PG_DB
PG_USER = config.PG_USER
PG_PASSWORD = config.PG_PASSWORD
PG_TABLE = 'zuora_invoiceitem_qa'
PG_SCHEMA = config.PG_SCHEMA
METADATA_TABLE = config.METADATA_TABLE
METADATA_OBJECT_NAME = 'zuora_invoiceitem_qa'

ZUORA_BASE_URL = 'https://rest.apisandbox.zuora.com'
ZUORA_CLIENT_ID = config.ZUORA_CLIENT_ID
ZUORA_CLIENT_SECRET = config.ZUORA_CLIENT_SECRET

POLL_INTERVAL_SECONDS = 30  # Poll Zuora every 30 seconds

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# -------- Global token cache --------
class ZuoraTokenCache:
    def __init__(self):
        self.token = None
        self.expiry = 0  # Unix timestamp

    def get_token(self):
        now = time.time()
        if self.token is None or now >= self.expiry:
            try:
                logging.info("Fetching new Zuora access token...")
                auth_url = f'{ZUORA_BASE_URL}/oauth/token'
                auth_payload = {
                    'grant_type': 'client_credentials',
                    'client_id': ZUORA_CLIENT_ID,
                    'client_secret': ZUORA_CLIENT_SECRET
                }
                response = requests.post(auth_url, data=auth_payload)
                response.raise_for_status()
                token_data = response.json()
                self.token = token_data['access_token']
                self.expiry = now + token_data.get('expires_in', 3600) - 60  # Refresh 1 min early
                logging.info(f"Obtained Zuora token, expires in {token_data.get('expires_in', 3600)} seconds.")
            except Exception as e:
                logging.error(f"Failed to fetch Zuora token: {e}")
                raise
        return self.token

zuora_token_cache = ZuoraTokenCache()

# -------- Postgres helpers --------
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logging.info("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def get_sqlalchemy_engine():
    try:
        user_enc = quote_plus(PG_USER)
        password_enc = quote_plus(PG_PASSWORD)
        connection_string = f'postgresql://{user_enc}:{password_enc}@{PG_HOST}:{PG_PORT}/{PG_DB}'
        engine = create_engine(connection_string)
        logging.info("SQLAlchemy engine created successfully.")
        return engine
    except Exception as e:
        logging.error(f"Failed to create SQLAlchemy engine: {e}")
        raise

def init_metadata_table(pg_conn):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (
                    object VARCHAR(255) PRIMARY KEY,
                    value TIMESTAMP WITHOUT TIME ZONE
                );
            """)
        pg_conn.commit()
        logging.info(f"Metadata table {METADATA_TABLE} initialized.")
    except Exception as e:
        logging.error(f"Error initializing metadata table: {e}")
        raise

def get_last_processed_value(pg_conn):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT value FROM {METADATA_TABLE} WHERE object = %s", (METADATA_OBJECT_NAME,))
            row = cur.fetchone()
            if row:
                logging.info(f"Last processed timestamp retrieved: {row[0]}")
            else:
                logging.info("No last processed timestamp found, starting fresh.")
            return row[0] if row else None
    except Exception as e:
        logging.error(f"Error retrieving last processed value: {e}")
        raise

def update_last_processed_value(pg_conn, last_value):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {METADATA_TABLE} (object, value) VALUES (%s, %s)
                ON CONFLICT (object) DO UPDATE SET value = EXCLUDED.value
            """, (METADATA_OBJECT_NAME, last_value))
        pg_conn.commit()
        logging.info(f"Updated last processed timestamp to: {last_value}")
    except Exception as e:
        logging.error(f"Error updating last processed value: {e}")
        raise

# -------- Zuora extraction --------
def extract_data_from_zuora(last_processed):
    try:
        access_token = zuora_token_cache.get_token()
        if last_processed:
            filter_condition = f"InvoiceItem.UpdatedDate >= '{last_processed.strftime('%Y-%m-%d')}'"
        else:
            filter_condition = "InvoiceItem.UpdatedDate >= '2025-01-01'"

        query_url = f'{ZUORA_BASE_URL}/v1/batch-query/'
        query_payload = {
            "queries": [
                {
                    "name": "invoiceitem",
                    "query": f"""
                                select Account.Id as AccountId,
                                Account.AccountNumber as AccountNumber,
                                Account.Status as AccountStatus,
                                Account.Name as AccountName,
                                Account.userId__c as UserId,
                                Subscription.Id AS SubscriptionId,
                                Subscription.Status as SubscriptionStatus,
                                Subscription.Name AS SubscriptionName,
                                Subscription.TermType AS SubscriptionTermType,
                                Subscription.RenewalTerm as RenewalTerm,
                                Subscription.Version AS SubscriptionVersion, 
                                Subscription.termStartDate AS SubscriptiontermStartDate, 
                                Subscription.termEndDate AS SubscriptiontermEndDate,
                                Product.Name as ProductName,
                                Product.ProductFamily__c as ProductFamily,
                                RatePlan.Id AS RateplanId, 
                                RatePlan.Name AS RatePlanName,
                                Invoice.Id as InvoiceId,
                                Invoice.Amount as InvoiceAmount,
                                Invoice.InvoiceNumber as InvoiceNumber,
                                Invoice.InvoiceDate as InvoiceDate,                               
                                Invoice.Status as InvoiceStatus,
                                Invoice.Currency as Currency,
                                InvoiceItem.Id as Id,
                                InvoiceItem.ChargeAmount as ChargeAmount,
                                InvoiceItem.ChargeDate as ChargeDate,
                                InvoiceItem.ChargeName as ChargeName,
                                InvoiceItem.TaxAmount as TaxAmount,
                                Account.CreatedDate as AccountCreatedDate,
                                Subscription.CreatedDate as SubscriptionCreatedDate,
                                Product.CreatedDate as ProductCreatedDate,
                                RatePlan.CreatedDate as RatePlanCreatedDate,
                                Invoice.CreatedDate as InvoiceCreatedDate,
                                InvoiceItem.CreatedDate as InvoiceItemCreatedDate,
                                InvoiceItem.UpdatedDate as UpdatedDate
                                from InvoiceItem 
                        where {filter_condition}
                        ORDER BY InvoiceItem.UpdatedDate ASC
                    """,
                    "type": "zoqlexport"
                }
            ]
        }
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        logging.info(f"Submitting Zuora query with filter: {filter_condition}")
        api_response = requests.post(query_url, headers=headers, json=query_payload)
        api_response.raise_for_status()
        query_job_id = api_response.json()['id']
        status_url = f'{ZUORA_BASE_URL}/v1/batch-query/jobs/{query_job_id}'

        status = None
        while status != 'completed':
            time.sleep(10)
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            status = status_response.json()['status']
            logging.info(f"Zuora job status: {status}")
            if status == 'failed':
                raise Exception(f"Zuora batch query job failed: {status_response.json()}")

        batches = status_response.json().get('batches')
        if not batches:
            logging.info("No batches found in Zuora response.")
            return pd.DataFrame()
        file_id = batches[0]['fileId']
        record_count = batches[0]['recordCount']
        logging.info(f"FileId: {file_id}, recordCount: {record_count}")
        if record_count == 0:
            logging.info("No new or updated records.")
            return pd.DataFrame()

        result_url = f'{ZUORA_BASE_URL}/v1/files/{file_id}'
        result_response = requests.get(result_url, headers={'Authorization': f'Bearer {access_token}'})
        result_response.raise_for_status()
        csv_data = result_response.text
        df = pd.read_csv(io.StringIO(csv_data), low_memory=False)
        df = df.drop_duplicates()
        logging.info(f"Extracted {len(df)} rows from Zuora.")
        return df
    except Exception as e:
        logging.error(f"Error extracting data from Zuora: {e}")
        raise

# -------- Kafka Setup --------
def delivery_report(err, msg):
    """Delivery callback for Kafka producer."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def setup_kafka_producer():
    """Create and return a Kafka producer."""
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)
    logging.info("Kafka producer created.")
    return producer

def setup_kafka_consumer():
    """Create and return a Kafka consumer."""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'zuora_live_consumer_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    logging.info("Kafka consumer created and subscribed to topic.")
    return consumer

def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    topic_list = [NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    fs = admin_client.create_topics(topic_list)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None if successful
            logging.info(f"Topic {topic} created successfully.")
        except Exception as e:
            if 'TopicAlreadyExistsError' in str(e) or 'TOPIC_ALREADY_EXISTS' in str(e):
                logging.info(f"Topic {topic} already exists. Continuing without error.")
            else:
                logging.error(f"Failed to create topic {topic}: {e}")
                # Raise or continue depending on your preference
                raise

def produce_dataframe_to_kafka(producer, df):
    """Produce each row of a pandas DataFrame as a JSON message to Kafka."""
    if df.empty:
        logging.info("No data to produce to Kafka.")
        return

    for i, row in enumerate(df.itertuples(index=False)):
        record_dict = row._asdict()
        json_str = json.dumps(record_dict)
        try:
            producer.produce(KAFKA_TOPIC, value=json_str, callback=delivery_report)
        except BufferError as e:
            logging.warning(f"Local producer queue is full ({e}), waiting to flush...")
            producer.poll(1)  # Wait to flush queue
            producer.produce(KAFKA_TOPIC, value=json_str, callback=delivery_report)
        if i % 1000 == 0:
            producer.poll(0)  # Serve delivery reports regularly to avoid queue buildup
    producer.flush()
    logging.info(f"Produced {len(df)} records to Kafka.")

# -------- Load data into Postgres with delete + insert --------
def load_df_to_postgres_with_delete(df, pg_conn):
    if df.empty:
        logging.info("No data to load into Postgres.")
        return

    engine = get_sqlalchemy_engine()
    inspector = inspect(engine)
    table_exists = inspector.has_table(PG_TABLE, schema=PG_SCHEMA)
    cursor = pg_conn.cursor()

    try:
        if table_exists:
            # Table exists, proceed with deletion if there are IDs
            ids_to_delete = df['Id'].tolist()
            if ids_to_delete:
                delete_query = f'''
                    DELETE FROM {PG_SCHEMA}."{PG_TABLE}"
                    WHERE "Id" IN ({', '.join(['%s'] * len(ids_to_delete))});
                '''
                cursor.execute(delete_query, ids_to_delete)
            else:
                logging.info("No IDs to delete.")

            # Insert new data appending to existing table
            logging.info(f"Inserting {len(df)} records into Postgres table {PG_SCHEMA}.{PG_TABLE} (append mode).")
            df.to_sql(name=PG_TABLE, con=engine, if_exists='append', index=False,
                      schema=PG_SCHEMA, chunksize=1000, method='multi')
            logging.info("Insertion completed.")

        else:
            # Table does not exist, create table by replacing (first load)
            logging.info(f"Table {PG_SCHEMA}.{PG_TABLE} does not exist. Creating table and loading data.")
            df.to_sql(name=PG_TABLE, con=engine, if_exists='replace', index=False,
                      schema=PG_SCHEMA, chunksize=1000, method='multi')
            logging.info("Table created and data loaded.")

        # Check if primary key exists, if not add it
        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = '{PG_SCHEMA}'
            AND table_name = '{PG_TABLE}';
        '''
        cursor.execute(pk_check_query)
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            logging.info(f"Primary key not found on table {PG_SCHEMA}.{PG_TABLE}. Adding primary key on 'Id'.")
            pk_query = f'''ALTER TABLE {PG_SCHEMA}."{PG_TABLE}" ADD PRIMARY KEY ("Id");'''
            cursor.execute(pk_query)
            pg_conn.commit()
            logging.info("Primary key added successfully.")
        else:
            logging.info("Primary key already exists on the table.")

    except Exception as e:
        logging.error(f"Error loading data into Postgres: {e}")
        raise
    finally:
        cursor.close()

# -------- Main live streaming loop --------
def live_streaming_pipeline():
    pg_conn = None
    producer = None
    consumer = None
    try:
        pg_conn = get_pg_connection()
        init_metadata_table(pg_conn)
        
        # Ensure Kafka topic exists before producing/consuming
        try:
            create_kafka_topic(KAFKA_TOPIC)
        except Exception as e:
            logging.error(f"Error creating Kafka topic: {e}")
            # Continue anyway assuming topic exists
        
        producer = setup_kafka_producer()
        consumer = setup_kafka_consumer()

        logging.info("Starting live streaming pipeline...")

        while True:
            try:
                last_processed = get_last_processed_value(pg_conn)
                df = extract_data_from_zuora(last_processed)

                if not df.empty:
                    # Produce messages properly serialized as JSON
                    produce_dataframe_to_kafka(producer, df)

                    # Update watermark
                    max_updated = df['UpdatedDate'].max()
                    try:
                        update_last_processed_value(pg_conn, max_updated)
                        logging.info(f"Watermark updated to {max_updated} (unconditionally).")
                    except Exception as e:
                        logging.error(f"Error updating watermark (ignored): {e}")
                else:
                    logging.info("No new data from Zuora, skipping watermark update.")

                # Consume messages in batch and load into Postgres
                consumed_msgs = []
                batch_size = 100
                batch_timeout_seconds = 10
                start_time = time.time()

                while True:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        if time.time() - start_time > batch_timeout_seconds:
                            break
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            logging.error(f"Kafka error: {msg.error()}")
                            break
                    raw_value = msg.value().decode('utf-8')
                    try:
                        record = json.loads(raw_value)
                        consumed_msgs.append(record)
                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decode error: {e} for message: {raw_value}")
                    consumer.commit(asynchronous=False)

                    if len(consumed_msgs) >= batch_size:
                        break

                if consumed_msgs:
                    df_consumed = pd.DataFrame(consumed_msgs)
                    load_df_to_postgres_with_delete(df_consumed, pg_conn)
                else:
                    logging.info("No messages consumed from Kafka.")

            except Exception as e:
                logging.error(f"Error in streaming loop iteration: {e}")

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logging.info("Shutdown requested by user.")

    except Exception as e:
        logging.error(f"Fatal error in pipeline: {e}")

    finally:
        if consumer:
            consumer.close()
        if pg_conn:
            pg_conn.close()
        logging.info("Live streaming pipeline stopped.")

if __name__ == '__main__':
    live_streaming_pipeline()
