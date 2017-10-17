from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

program = Blueprint('program', __name__)

@program.route('/api/program')
def program_list():
    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    programs = DimProgram.query.all()

    return jsonify(total=len(programs), programs=[p.serialize() for p in programs])