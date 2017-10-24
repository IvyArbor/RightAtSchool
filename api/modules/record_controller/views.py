from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

record = Blueprint('record', __name__)

@record.route('/api/record')
def record_list():

    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    record = FactRecord.query.all()

    return jsonify(total=len(record), record=[c.serialize() for c in record])