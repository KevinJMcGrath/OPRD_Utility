import logging
import simple_salesforce

import config

class SFDCClient:
    def __init__(self, simple_salesforce_client: simple_salesforce.Salesforce):
        self.inner_client: simple_salesforce.Salesforce = simple_salesforce_client

    def query(self, soql: str):
        return self.inner_client.query_all(soql)['records']


def init_client_from_config():
    n = config.Salesforce.name
    u = config.Salesforce.username
    p = config.Salesforce.password
    s = config.Salesforce.security_token
    d = config.Salesforce.domain

    logging.info(f'Logging into SFDC client ({n})')
    ss = simple_salesforce.Salesforce(username=u, password=p, security_token=s, domain=d, version='52.0')

    return SFDCClient(ss)