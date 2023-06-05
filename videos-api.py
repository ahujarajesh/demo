import os
import json
import http.server
from redis import Redis
import time
import strings
import random
import opentracing
import context_transport
from jaeger_client import Config
from opentracing.ext import tags

service_name = "videos-api"

environment = os.getenv("ENVIRONMENT")
redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
otel_collector_endpoint = os.getenv("OTEL_COLLECTOR_ENDPOINT")
flaky = os.getenv("FLAKY")
delay = os.getenv("DELAY")

tracer_config = Config(
    config={
        "sampler": {
            "type": "const",
            "param": 1,
        },
        "logging": True,
        "local_agent": {
            "reporting_host": otel_collector_endpoint.split(":")[0],
            "reporting_port": int(otel_collector_endpoint.split(":")[1]),
        },
    },
    service_name=service_name,
)

tracer = tracer_config.initialize_tracer()

class VideosRequestHandler(http.server.BaseHTTPRequestHandler):

    def _set_response(self):
        if environment == "DEBUG":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, X-MY-API-Version")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

    def do_GET(self):
        self._set_response()
        extracted_context = context_transport.extract_context(request)
        with tracer.start_span("videos-api: GET /id", context=extracted_context) as span:
            span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_SERVER)

            if flaky == "true" and random.randint(0, 89) < 30:
                raise Exception("flaky error occurred")

            video = self.get_video(span)

            if "jM36M39MA3I" in video and delay == "true":
                time.sleep(6)

            self.cors()
            self.wfile.write(video.encode("utf-8"))

    def get_video(self, parent_span):
        with tracer.start_span("videos-api: redis-get", child_of=parent_span) as span:
            id = self.path[1:]
            video_data = redis_client.get(id)
            if video_data is None:
                span.set_tag(tags.ERROR, True)
                headers = {}
                tracer.inject(span.context, opentracing.Format.HTTP_HEADERS, headers)
                return "{}"

            headers = {}
            tracer.inject(span.context, opentracing.Format.HTTP_HEADERS, headers)
            return video_data.decode("utf-8")

    def cors(self):
        if environment == "DEBUG":
            self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, X-MY-API-Version")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Access-Control-Allow-Origin", "*")

def run_server():
    server_address = ("", 10010)
    httpd = http.server.HTTPServer(server_address, VideosRequestHandler)
    print("Running...")
    httpd.serve_forever()

if __name__ == "__main__":
    redis_client = Redis(host=redis_host, port=int(redis_port), db=0)
    run_server()
