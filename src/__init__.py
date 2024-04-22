from flask import Flask, render_template
from .routes import api_routes
import os


def generate_app() -> Flask:
    app = Flask(__name__)
    if not os.getenv("PRO_PUBLICA_API_KEY"):
        from dotenv import load_dotenv
        load_dotenv()
    assert os.getenv("PRO_PUBLICA_API_KEY") is not None
    app.secret_key = 'p33p33p00p00'
    return app

app = generate_app()
app.register_blueprint(api_routes)

@app.route('/', methods=['GET'])
def get_routes() -> str:
    # Create available routes UI on home page.
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            "endpoint": rule.endpoint,
            "methods": list(rule.methods),
            "url": str(rule)
        })
    return render_template('routes.html', routes=routes)