from flask import Flask, request
from flask_restful import abort, Api, Resource

import redis
import os, json

#Configs
app_host = os.environ.get("APP_HOST","0.0.0.0")
app_port = int(os.environ.get("APP_PORT",9090))
app_log_level = os.environ.get("APP_LOG_LEVEL","WARN")

redis_server = os.environ.get("REDIS_SERVER","localhost")
redis_port = int(os.environ.get("REDIS_PORT",6379))

#Inits
app = Flask(__name__)
api = Api(app)

app.logger.setLevel(app_log_level)

redis_client = redis.StrictRedis(host=redis_server,port=redis_port,decode_responses=True)

def update_to_redis(in_event_key,in_event_json_str_value,in_ex=None,in_expat=None):
    ret=redis_client.set(name=in_event_key,value=in_event_json_str_value,ex=in_ex,exat=in_expat,xx=True)
    app.logger.info(f'output of update key {in_event_key} value {in_event_json_str_value} is {ret}')
    if ret is None:
        abort(404, message="Event [{}] doesn't exist".format(in_event_key))

def add_to_redis(in_event_key,in_event_json_str_value,in_ex=None,in_expat=None):
    redis_client.set(name=in_event_key,value=in_event_json_str_value,ex=in_ex,exat=in_expat)

def request_validations(in_request):
    content_type=in_request.headers.get("Content-Type")
    if content_type != "application/json":
        abort(415, message="Request contains Unsupported Media Type [{}]".format(content_type))

# shows a single event item and lets you delete a event item
class Events(Resource):
    def get(self, key_id):
        app.logger.info(f'Enter Get Event record with key_id {key_id}')
        resp = redis_client.get(name=key_id)
        if resp is None:
            abort(404, message="Event [{}] doesn't exist".format(key_id))
        return json.loads(resp)

    def delete(self, key_id):
        app.logger.info(f'Enter Delete Event record  with key_id {key_id}')
        del_rec=redis_client.getdel(key_id)
        if del_rec is None:
            abort(404, message="Event [{}] doesn't exist".format(key_id))
        message="Operation Succeeeded. Record [{}] deleted successfully".format(json.loads(del_rec))
        return message, 200

    def put(self, key_id):
        app.logger.info(f'Enter Put Event record with key_id {key_id}')
        request_validations(request)
        req_data_json=request.get_json()
        update_to_redis(key_id,json.dumps(req_data_json))
        return "Operation Succeeeded", 201

# shows a list of all events, and lets you POST to add new event
class EventsList(Resource):
    def get(self):
        app.logger.info(f'Enter get/List Events in system')
        key_ids=redis_client.keys()
        #lb_str = lambda x:x.decode()
        return key_ids

    def post(self):
        app.logger.info(f'Enter Add/Create Event record')
        request_validations(request)
        req_data_json=request.get_json()
        app.logger.info(f'data received ${req_data_json}')
        for key in req_data_json.keys():
            add_to_redis(key,json.dumps(req_data_json[key]))
        #redis_client.mset()
        return "Operation Succeeeded", 201

class EventsPublish(Resource):
    def post(self):
        app.logger.info(f'Enter Publish Message for Redis Pub/Sub')
        request_validations(request)
        req_data_json=request.get_json()
        app.logger.info(f'data received ${req_data_json}')
        redis_client.publish('cust_notification',json.dumps(req_data_json))
        return "Operation Succeeeded", 201

## Actually setup the Api resource routing here
api.add_resource(EventsList, '/events')
api.add_resource(Events, '/events/<key_id>')
api.add_resource(EventsPublish, '/events/publish/')


# shows a single event item and lets you delete a event item
class StreamEvents(Resource):
    def get(self, stream_channel, stream_count=None):
        app.logger.info(f'Enter Get / list Stream Events for the stream channel {stream_channel}')
        resp = redis_client.xrange(name=stream_channel,count= int(stream_count) if stream_count!=None else None)
        app.logger.info(f'the response from xrange {resp}')
        if resp is None:
            abort(404, message="Event [{}] doesn't exist".format(stream_channel))
        return resp

    def delete(self, stream_channel):
        app.logger.info(f'Enter Delete Stream channel {stream_channel}')
        del_rec=redis_client.delete(stream_channel)
        app.logger.info(f'Deleted event channel response {del_rec}')
        if del_rec is None or del_rec==0:
            abort(404, message="Event [{}] doesn't exist".format(stream_channel))
        
        message="Operation Succeeeded. Record [{}] deleted successfully".format(stream_channel)
        return message, 200

    def post(self,stream_channel):
        app.logger.info(f'Enter post/add/create Stream Events for the stream channel {stream_channel}')
        request_validations(request)
        req_data_json=request.get_json()
        app.logger.info(f'data received ${req_data_json}')
        redis_client.xadd(name=stream_channel,fields=req_data_json)
        #redis_client.mset()
        return "Operation Succeeeded", 201

api.add_resource(StreamEvents, '/stream/events/<stream_channel>','/stream/events/<stream_channel>/<stream_count>')

if __name__ == '__main__':
    from waitress import serve
    #app.run(port=app_port,debug=True)
    serve(app,host=app_host,port=app_port)
