import os

from flask import Flask, render_template, url_for

from .api_routes import api_routes
from .routes import main_routes


def generate_app() -> Flask:
    app = Flask(__name__)
    if not os.getenv("PRO_PUBLICA_API_KEY"):
        from dotenv import load_dotenv
        load_dotenv()
    assert os.getenv("PRO_PUBLICA_API_KEY") is not None
    app.secret_key = 'p33p33p00p00'
    return app


app = generate_app()
for routes in [main_routes, api_routes]:
    app.register_blueprint(routes)
