from flask import Flask
import os
import configparser


def create_app():
    APP_DIR = os.path.abspath(os.path.dirname(__file__))
    app = Flask(__name__)

    config = configparser.ConfigParser()
    config.read(os.path.abspath(os.path.join(".ini")))

    prod_section = config['PROD']
    prod_db_name = prod_section['MONGODB_NAME']
    app.config['PROD_DB'] = prod_db_name
    api_fetch_limit = config['PROD']['TRACK_API_FETCH_LIMIT']
    app.config['API_FETCH_LIMIT'] = api_fetch_limit
    backup_db_name = config['BACKUP']['MONGODB_NAME']
    app.config['BACKUP_DB'] = backup_db_name
    test_db_name = config['TEST']['MONGODB_NAME']
    app.config['TEST_DB'] = test_db_name

    from .main.views import main as main_blueprint
    app.register_blueprint(main_blueprint)

    @app.route('/', defaults={'path': ''})
    def serve(path):
        return render_template('index.html')

    return app

