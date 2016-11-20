
from flask import current_app, Blueprint, render_template


admin = Blueprint('admin', __name__,
                  url_prefix='/admin',
                  template_folder='templates')

@admin.route('/')
def index():
    return render_template('admin/index.html', title='Admin index')
