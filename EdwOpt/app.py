from __future__ import print_function

from flask_socketio import SocketIO
from metricminer import create_app

app = create_app()
socketio = SocketIO(app)

@socketio.on('message')
def handle_message(message):
    print('received message: ' + message)

@socketio.on('report')
def handle_report(report):
    print('received report: %s' % report)
    socketio.emit('sql', 'Select a \nfrom b %s' % report)

@socketio.on('sql')
def handle_report(sql):
    print('got sql : %s' % sql)
    socketio.emit('result', 'Rows: (%s)' % sql)

socketio.run(app, debug=True, use_reloader=True)