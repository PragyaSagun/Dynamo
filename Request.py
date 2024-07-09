class Request:
    def __init__(self,action,key,value=None,request_no=None,client=None):
        self.action=action
        self.key=key
        self.value=value
        self.request=request_no
        self.client=client
