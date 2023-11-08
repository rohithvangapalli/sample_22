import requests
import logging
import re
import base64
import sys
import datetime
from pyspark.sql import HiveContext, SparkSession

spark = SparkSession \
    .builder \
    .appName("creds_test") \
    .getOrCreate()


def get_password(token, cred_id, cred_type, fireshots_url):
    payload = {"credential_id": cred_id, "credential_type_id": cred_type}
    url = '{0}'.format(fireshots_url)
    r = requests.post(url, headers={"Content-Type": "application/json", "Authorization": token, "Accept": "application/json"}, json=payload)
    if r.status_code == 200:
        logging.info("[+] Password obtained [+]")
        return r.json()
    else:
        logging.info(r.json())


if __name__ == "__main__":
    #args = spark.conf.get("spark.driver.args").split(",")
    #args = spark.conf.get("spark.driver.args").split("\\s+")
    token = spark.conf.get("spark.nabu.token")
    fireshots_uri = spark.conf.get("spark.nabu.fireshots_url")
    
    dev_cred_id = 1
    dev_cred_type = 1
    print(dev_cred_id,dev_cred_type)

    dev_creds = get_password(token, dev_cred_id, dev_cred_type, fireshots_uri)
    print(dev_creds)
    print(dev_creds["data"])
