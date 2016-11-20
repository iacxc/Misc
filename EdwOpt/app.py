from __future__ import print_function

from metricminer import create_app, create_io

app = create_app()

socketio = create_io(app)

socketio.run(app, debug=True, use_reloader=True)