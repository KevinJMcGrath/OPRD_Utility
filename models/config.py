class SalesforceSettings:
    def __init__(self, sfdc_json):
        self.name = 'Prod'
        self.username = sfdc_json['username']
        self.password = sfdc_json['password']
        self.security_token = sfdc_json['security_token']
        self.domain = 'login'