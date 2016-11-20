
from flask import Flask

def create_app():
    app = Flask(__name__)
    app.config['SCRET_KEY'] = 'scret!'

    from metricminer.blueprints import admin
    from metricminer.blueprints import miner

    app.register_blueprint(admin)
    app.register_blueprint(miner)

    return app
