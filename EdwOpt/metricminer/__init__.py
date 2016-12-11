
from datetime import datetime
#from flask_socketio import SocketIO
#from flask import Flask, render_template

from metricminer.models import SqDB, sqlstmt


#def create_app():
#    app = Flask(__name__)
#    app.config['SCRET_KEY'] = 'scret!'
#
#    from metricminer.blueprints import admin
#    from metricminer.blueprints import miner
#
#    app.register_blueprint(admin)
#    app.register_blueprint(miner)
#
#    return app


#def create_io(app):
#    socketio = SocketIO(app)
#
#    @socketio.on('message')
#    def handle_message(message):
#        print('received message: ' + message)
#
#    @socketio.on('report')
#    def get_sql(report):
#        print('received report: %s' % report)
#        socketio.emit('sql',sqlstmt.get_sql(report))
#
#    @socketio.on('query')
#    def run_query(query):
#        print('received query')
#
#        params = []
#        if query['start_ts'].strip():
#            start_ts = datetime.strptime(query['start_ts'], '%Y-%m-%d %H:%M:%S')
#            params.append(start_ts)
#
#        if query['end_ts'].strip():
#            end_ts   = datetime.strptime(query['end_ts'], '%Y-%m-%d %H:%M:%S')
#            params.append(end_ts)
#
#        try:
#            db = SqDB.Database(query['dsn'], query['uid'], query['pwd'])
#
#            result = db.getall(query['sql'], *params)
#
#            socketio.emit('result', render_template('datatable.html', **result))
#        except Exception as exp:
#            socketio.emit('result', str(exp))
#
#
#    return socketio
