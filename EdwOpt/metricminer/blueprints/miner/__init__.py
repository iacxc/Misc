
from flask import Blueprint, render_template


miner = Blueprint('miner', __name__)

@miner.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html', title='Metric Miner Reports')
