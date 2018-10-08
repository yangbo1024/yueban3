yueban3

A light-weight distribute game server based Python 3.5+

start example(use gunicorn):
    gunicorn my_app_module:my_web_app -w 4 --bind localhost:8080 --worker-class aiohttp.GunicornWebWorker
