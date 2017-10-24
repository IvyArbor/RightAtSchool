from flask import Blueprint, jsonify, request
from modules.models import *
from modules.config import *

course = Blueprint('course', __name__)

@course.route('/api/course')
def course_list():

    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    course = DimCourse.query.all()

    return jsonify(total=len(course), course=[c.serialize() for c in course])