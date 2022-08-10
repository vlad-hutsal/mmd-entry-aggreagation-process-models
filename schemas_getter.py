import json
import os
from typing import Dict

from google.cloud.sql.connector import Connector, IPTypes
import pg8000

import sqlalchemy as sq
import logging

logger = logging.getLogger("schemas_getter")

DB_USER = "mm_readonly"  # e.g. 'my-db-user'
DB_PASS = "mmdtest123"  # e.g. 'my-db-password'
DB_NAME = "mmd-mus-main-dev"  # e.g. 'my-database'

table_names = [
    'album',
    'amg_desc',
    'association',
    'attribute',
    'attributeassociation',
    'attributelink',
    'audio',
    'audiosample',
    'composition',
    'compositionversion',
    'document',
    'event',
    'image',
    'imagelink',
    'location',
    'log_activity',
    'log_datachanges',
    'log_datachangesarchive',
    'log_proccall',
    'media',
    'msreplication_objects',
    'msreplication_subscriptions',
    'mssubscription_agents',
    'mssubscription_properties',
    'name',
    'part',
    'performance',
    'release',
    'repltest',
    'set',
    'toc',
    'track',
    'trackattributelink',
    'trackquantitativevalues',
    'vendorlink',
]

music_entry = [
    'CopyOfGUID',
    'AlbumTitle',
    'UPC',
    'PR_ID',
    'MusicType',
    'NumDiscs',
    'HasMediaAudio',
    'HasMediaImages',
    'CreationDT',
    'EntryXML',
    'IngestionDate',
    'AnalyzeDate',
    'AnalyzeAMGDate',
    'CreatedByIP',
    'CreatedByUserID',
    'CreatedByUsername',
    'GUIDAlbumID',
    'AlbumID',
    'LockDate',
    'LockKey',
    'IntegrationDate',
    'IntegrationStatus',
    'Status',
    'ProcessingDate',
    'ProcessingDataSetTemp',
    'ProcessingDataSet',
 ]

# connect_with_connector initializes a connection pool for a
# Cloud SQL instance of Postgres using the Cloud SQL Python Connector.
def connect_with_connector() -> sq.engine.base.Engine:
    # Note: Saving credentials in environment variables is convenient, but not
    # secure - consider a more secure solution such as
    # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
    # keep secrets safe.

    instance_connection_name = "mmd-dev-1:us-central1:mmd-ship1-dev"  # e.g. 'project:region:instance'
    db_user = "mm_readonly"  # e.g. 'my-db-user'
    db_pass = "mmdtest123"  # e.g. 'my-db-password'
    db_name = "mmd-mus-main-dev"  # e.g. 'my-database'

    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
        )
        return conn

    # The Cloud SQL Python Connector can be used with SQLAlchemy
    # using the 'creator' argument to 'create_engine'
    pool = sq.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        # ...
    )
    return pool


def get_db_response_gen(conn: sq.engine.base.Engine):
    while len(table_names):
        table_name = table_names.pop(0).lower()
        q = f"SELECT column_name FROM information_schema.columns WHERE TABLE_NAME = '{table_name}';"
        res = conn.execute(q).all()
        logger.info(f'Working on {table_name}. len(table_names) tables left')
        yield (table_name, [x.lower() for (x,) in res])


def write_schemas_to_json(all_schemas: Dict, path = "AMGMusicSchemas.json"):
    with open(path, "w+") as fl:
        json.dump(all_schemas, fl, indent=4)


def get_all_schemas(conn: sq.engine.base.Engine):
    return {t_name: val for t_name, val in get_db_response_gen(conn)}


def run():
    conn = connect_with_connector()
    schemas = get_all_schemas(conn)
    res = {}

    global music_entry
    me = [e.lower() for e in music_entry]
    for field in me:
        for schema, col_names in schemas.items():
            if field in col_names:
                res[field] = schema
    
    print('ex\n\n', res)
