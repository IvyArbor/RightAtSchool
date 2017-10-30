from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

deals = Blueprint('deals', __name__)

@deals.route('/api/deals')
def deals_list():

    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    deals = FactDeals.query.all()

    return jsonify(total=len(deals), deals=[c.serialize() for c in deals])