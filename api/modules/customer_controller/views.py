from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

customer = Blueprint('customer', __name__)

@customer.route('/api/customers')
def customer_list():
    # customers = customer_dimension.query.with_entities(customer_dimension.firstname, customer_dimension.lastname).all()
    # customers = DimCustomer.query.all()

    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    customers = DimCustomer.query.all()

    return jsonify(total=len(customers), customers=[c.serialize() for c in customers])