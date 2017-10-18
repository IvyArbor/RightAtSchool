from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

activity = Blueprint('activity', __name__)

@activity.route('/api/activity')
def activity_list():
    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    activities = DimActivity.query.all()

    return jsonify(total=len(activities), activities=[a.serialize() for a in activities])