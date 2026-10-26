"""Жижиг вэб сервер: UI (static/index.html), /cam/ proxy, мөн GitHub Pages дээрх статик
файлуудыг (data.json, stations/<hash>.json) шууд benzin.mn-ээс татаж үүсгэнэ.

Ажиллуулах:  python3 server.py  [--port 8000]
"""

import argparse
import json
import os
import urllib.error
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import crawler

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
# Камерын HLS stream зөвхөн ubtraffic.mn-д CORS нээлттэй тул native HLS-гүй
# браузер (Firefox г.м.) hls.js-ээр энэ proxy-оор дамжуулж тоглуулна.
CAM_HOST = "https://stream.ubtraffic.mn"

# stations/<hash>.json → ШТС-ын id; data.json бүрт шинэчлэгдэнэ.
_ids_by_path = {}


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC, **kwargs)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/data.json":
            self._api(self._data)
        elif url.path.startswith("/stations/"):
            sid = _ids_by_path.get(url.path.lstrip("/"))
            if not sid:
                self._json(404, {"error": "Жагсаалтыг дахин шинэчилнэ үү"})
            else:
                self._api(lambda: crawler.station_detail(sid))
        elif url.path.startswith("/cam/live/"):
            self._cam(url.path[len("/cam"):])
        else:
            super().do_GET()

    @staticmethod
    def _data():
        data = crawler.build()
        for r in data["refilled"] + data["selling92"]:
            _ids_by_path[r["detail"]] = r["id"]
        data["cam_proxy"] = True
        return data

    def _api(self, fn):
        try:
            self._json(200, fn())
        except Exception as e:  # benzin.mn унтарсан, сүлжээ тасарсан г.м.
            self._json(502, {"error": f"benzin.mn-ээс татаж чадсангүй: {e}"})

    def _cam(self, path):
        if ".." in path or not path.endswith((".m3u8", ".ts")):
            self.send_error(404)
            return
        req = urllib.request.Request(CAM_HOST + path, headers={"User-Agent": crawler.UA})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                body = r.read()
                ctype = r.headers.get("Content-Type", "application/octet-stream")
        except urllib.error.HTTPError as e:
            self.send_error(e.code)  # Wowza 480p-г асаах хооронд 404 өгдөг
            return
        except Exception:
            self.send_error(502)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        if not self.path.startswith("/cam/"):  # камерын chunk бүрийг бичихгүй
            super().log_message(fmt, *args)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"http://127.0.0.1:{args.port} дээр ажиллаж байна (Ctrl+C зогсооно)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
