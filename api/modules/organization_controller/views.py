from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

organization = Blueprint('organization', __name__)

@organization.route('/api/organization')
def organization_list():

    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    organization = DimOrganization.query.all()

    return jsonify(total=len(organization), organization=[c.serialize() for c in organization])