
from flask_socketio import SocketIO
from flask import Flask, render_template, flash

from metricminer.models import SqDB


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
    def handle_report(report):
        print('received report: %s' % report)
        socketio.emit('sql', 'Select a \nfrom b %s' % report)

    @socketio.on('query')
    def handle_report(query):
        print('received query')
        db = SqDB.Database(query['dsn'], query['uid'], query['pwd'])

        result = db.getall(query['sql'])

        socketio.emit('result', render_template('datatable.html', **result))

    return socketio
