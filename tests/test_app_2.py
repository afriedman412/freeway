from flask_testing import TestCase

from app import app


class TestFolio(TestCase):
    def create_app(self):
        return app

    def test_route(self):
        response = self.client.get("/")
        self.assert200(response)

    def test_endpoints(self):
        for endpoint in [
            "/",
            "/committee/C00864215",
            "/filings/date/2024-3-25"
        ]:
            endpoint_response = self.client.get(endpoint)
            self.assert200(endpoint_response)
