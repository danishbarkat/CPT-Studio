import os

# Import the Flask app
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app import app

# Ensure template folder is discoverable when running from /api
os.chdir(os.path.dirname(os.path.dirname(__file__)))
