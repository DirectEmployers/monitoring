# Standard Library
from elasticsearch import Elasticsearch
from email.mime.text import MIMEText
import smtplib

# Local imports
import secrets

# Logging
import logging
logger = logging.getLogger(__name__)




def connect():
    es = Elasticsearch(secrets.ELASTICSEARCH['host'],
                       port=secrets.ELASTICSEARCH['port'])
    return es


def send_email():
    msg = MIMEText("We've not seen reports of ETL jobs being processed on %s." % secrets.ELASTICSEARCH['host'])
    msg['Subject'] = 'ETL tasks may not be processing'
    msg['From'] = 'monitoring@my.jobs'
    msg['To'] = 'aws@directemployers.org'
    
    s = smtplib.SMTP(host=secrets.EMAIL['HOST'], port=secrets.EMAIL['PORT'])
    s.login(secrets.EMAIL["USER"], secrets.EMAIL["PASSWORD"])
    s.sendmail('monitoring@my.jobs', ['aws@directemployers.org'], msg.as_string())
    s.quit()


if __name__ == '__main__':
    logging.basicConfig(level="INFO")
    es = connect()
    query = { "query" : {
             "filtered": {
                          "query":{
                                   "query_string": {"query": "record:succeed in AND tasks.etl_to_solr"}
                                   },
                            "filter": {
                                       "range": { "@timestamp": {'gte': "now-6h"}}
                                       }
                          }
            }
    }
    results = es.search(None, body=query)
    if results['hits']['total'] <= 0:
        logger.warn("No events found.")
        send_email()