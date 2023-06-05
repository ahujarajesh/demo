import os
import json
import requests
from redis import Redis
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource
from http.server import BaseHTTPRequestHandler, HTTPServer

service_name = "playlists-api"

environment = os.getenv("ENVIRONMENT")
redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
otel_collector_endpoint = os.getenv("OTEL_COLLECTOR_ENDPOINT")

resource = Resource.create(
    attributes={
        "service.name": service_name
    }
)

trace.set_tracer_provider(TracerProvider(resource=resource))

span_exporter = OTLPSpanExporter(endpoint=otel_collector_endpoint)
span_processor = BatchExportSpanProcessor(span_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

redis_client = Redis(host=redis_host, port=int(redis_port), db=0)

class PlaylistsRequestHandler(BaseHTTPRequestHandler):

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

        with trace.get_tracer(__name__).start_as_current_span("playlists-api: GET /") as span:
            playlists_json = get_playlists()

            playlists = json.loads(playlists_json)

            for playlist in playlists:
                vs = []
                for video in playlist["videos"]:
                    with trace.get_tracer(__name__).start_as_current_span("playlists-api: videos-api GET /id") as span:
                        url = f"http://videos-api:10010/{video['id']}"
                        headers = {}
                        try:
                            response = requests.get(url, headers=headers)
                            video_data = response.json()
                            vs.append(video_data)
                        except Exception as e:
                            print(e)
                            span.set_attribute("error", True)
                            break

                playlist["videos"] = vs

        playlists_bytes = json.dumps(playlists).encode("utf-8")
        self.wfile.write(playlists_bytes)

def get_playlists():
    with trace.get_tracer(__name__).start_as_current_span("playlists-api: redis-get") as span:
        playlist_data = redis_client.get("playlists")
        if not playlist_data:
            print("error occurred retrieving playlists from Redis")
            span.set_attribute("error", True)
            return "[]"
        return playlist_data.decode("utf-8")

def run_server():
    server_address = ("", 10010)
    httpd = HTTPServer(server_address, PlaylistsRequestHandler)
    print("Running...")
    httpd.serve_forever()

if __name__ == "__main__":
    run_server()

