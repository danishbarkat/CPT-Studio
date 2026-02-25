import os

from vercel_wsgi import handle

# Import the Flask app
from app import app


def handler(event, context):
    """
    Vercel serverless entrypoint.
    """
    # Ensure template folder is discoverable when running from /api
    os.chdir(os.path.dirname(os.path.dirname(__file__)))
    return handle(event, context, app)
