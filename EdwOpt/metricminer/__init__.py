
from datetime import datetime
from flask_socketio import SocketIO
from flask import Flask, render_template, flash

from metricminer.models import SqDB, sqlstmt


def create_app():
    app = Flask(__name__)
    app.config['SCRET_KEY'] = 'scret!'

    from metricminer.blueprints import admin
    from metricminer.blueprints import miner

    app.register_blueprint(admin)
    app.register_blueprint(miner)

    return app


def create_io(app):
    socketio = SocketIO(app)

    @socketio.on('message')
    def handle_message(message):
        print('received message: ' + message)

    @socketio.on('report')
    def get_sql(report):
        print('received report: %s' % report)
        socketio.emit('sql',sqlstmts.get_sql(report))

    @socketio.on('query')
    def run_query(query):
        print('received query')
        start_ts = datetime.strptime(query['start_ts'], '%Y-%m-%d %H:%M:%S')
        end_ts   = datetime.strptime(query['end_ts'], '%Y-%m-%d %H:%M:%S')
        try:
            db = SqDB.Database(query['dsn'], query['uid'], query['pwd'])

            result = db.getall(query['sql'], start_ts, end_ts)
            socketio.emit('result', '<table class="data" cellpadding="2" cellspacing="2">\n')
            socketio.emit('result', '<tr class="head">\n')
            for field in result['fields']:
                socketio.emit('result', '<th class="head">%s</th>\n' % field)

            socketio.emit('result', '</tr>\n')
            for idx, row in enumerate(rows):
                if idx % 2 == 0:
                    socketio.emit('result', '<tr class="alter">\n')
                else:
                    socketio.emit('result', '<tr class="data">\n')

                for value in row:
                    socketio.emit('result', '<td class="data">%s</td>\n' % value)

                socketio.emit('result', '</tr>\n')

            socketio.emit('result', '</table>\n')

#            socketio.emit('result', render_template('datatable.html', **result))
        except Exception as exp:
            socketio.emit('result', str(exp))


    return socketio
