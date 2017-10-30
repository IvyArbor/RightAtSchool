from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

people = Blueprint('people', __name__)

@people.route('/api/people')
def people_list():

    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    people = DimPeople.query.all()

    return jsonify(total=len(people), people=[c.serialize() for c in people])