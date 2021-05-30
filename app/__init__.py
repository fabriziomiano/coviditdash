"""
Flask application factory
"""
import logging
import os

import click
from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv
from flask import Flask, request, render_template, send_from_directory
from flask_babel import Babel
from flask_compress import Compress
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_pymongo import PyMongo
from flask_sitemap import Sitemap
from flask_sqlalchemy import SQLAlchemy

from config import config as app_config
from settings import TRANSLATION_DIRNAME, LANGUAGES

load_dotenv()
mongo = PyMongo()
babel = Babel()
sitemap = Sitemap()
compress = Compress()
celery = Celery(__name__)
db = SQLAlchemy()
limiter = Limiter(key_func=get_remote_address)


@babel.localeselector
def get_locale():
    """Return the locale that best match the client request"""
    return request.accept_languages.best_match(LANGUAGES.keys())


def set_error_handlers(app):
    """Error handler"""

    @app.errorhandler(404)
    def page_not_found(e):
        """Page not found"""
        app.logger.error("{}".format(e))
        return render_template("errors/404.html", error=e), 404

    @app.errorhandler(500)
    def server_error(e):
        """Generic server error"""
        app.logger.error("{}".format(e))
        return render_template("errors/generic.html", error=e)


def set_robots_txt_rule(app):
    """Bots rule"""

    @app.route("/robots.txt")
    def robots_txt():
        """Serve the robots.txt file"""
        return send_from_directory(app.static_folder, request.path[1:])


def set_favicon_rule(app):
    """Favicon rule"""

    @app.route("/favicon.ico")
    def favicon():
        """Serve the favicon.ico file"""
        return send_from_directory(
            os.path.join(app.root_path, "static"),
            "favicon.ico", mimetype="image/vnd.microsoft.icon")


def get_environment():
    """Return app environment"""
    return os.environ.get('APPLICATION_ENV') or 'development'


def create_app():
    """Create the flask application"""
    env = get_environment()
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)
    app.config.from_object(app_config[env])
    app.config["BABEL_TRANSLATION_DIRECTORIES"] = os.path.join(
        app.root_path, TRANSLATION_DIRNAME)
    compress.init_app(app)
    mongo.init_app(app)
    db.init_app(app)
    babel.init_app(app)
    sitemap.init_app(app)
    set_error_handlers(app)
    set_robots_txt_rule(app)
    set_favicon_rule(app)
    limiter.init_app(app)
    celery.config_from_object(app.config)
    celery.conf.update(app.config.get("CELERY_CONFIG", {}))
    celery.conf.beat_schedule = {
        'istat-population-update': {
            'task': 'app.db_tools.tasks.update_istat_it_population_collection',
            'schedule': crontab(hour=0, minute=0)
        }
    }

    from .ui import pandemic, vaccines
    app.register_blueprint(pandemic)
    app.register_blueprint(vaccines)

    from .api import api
    app.register_blueprint(api)

    from app.db_utils.create import DBObjectsCreator

    dboc = DBObjectsCreator()

    creation_menu = {
        "national": dboc.create_national_table,
        "national-trends": dboc.create_national_trends_view,
        "regional": dboc.create_regional_table,
        "regional-trends": dboc.create_regional_trends_view,
        "regional-breakdown": dboc.create_regional_breakdown_view,
        "provincial-trends": dboc.create_provincial_trends_view,
        "provincial": dboc.create_provincial_table,
        "admins": dboc.create_vax_admins_table,
        "summary": dboc.create_vax_summary_table,
        "admins-summary": dboc.create_vax_admins_summary_table

    }

    @app.after_request
    def add_header(r):
        """
        Add headers to both force latest IE rendering engine or Chrome Frame,
        and also to cache the rendered page for 10 minutes.
        """
        r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        r.headers["Pragma"] = "no-cache"
        r.headers["Expires"] = "0"
        r.headers["Cache-Control"] = "public, max-age=0"
        return r

    @app.cli.command("create-db")
    def populate_db():
        """Create the objects in the DB"""
        for _type in creation_menu:
            creation_menu[_type]()

    @app.cli.command("create")
    @click.argument("collection_type")
    def populate_collection(collection_type):
        """Create a specific object on DB"""
        allowed_types = [k for k in creation_menu]
        try:
            creation_menu[collection_type]()
        except KeyError:
            app.logger.error(
                f"Invalid collection type: {collection_type}. " +
                "Allowed types: [" + ", ".join(a for a in allowed_types) + "]"
            )

    return app
