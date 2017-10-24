from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

user = Blueprint('user', __name__)

@user.route('/api/user')
def user_list():

    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    user = DimUser.query.all()

    return jsonify(total=len(user), course=[c.serialize() for c in user])