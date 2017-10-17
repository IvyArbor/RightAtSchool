from flask import Blueprint, jsonify, request
from sqlalchemy.sql import func
from modules.models import *
from modules.config import *

activity_enrollment = Blueprint('activity_enrollment', __name__)

@activity_enrollment.route('/api/activity_enrollment')
def activity_enrollment_list():
    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})


    activity_enrollment = FactActivityEnrollment.query.all()

    return jsonify(total=len(activity_enrollment), activity_enrollment=[a.serialize() for a in activity_enrollment])


@activity_enrollment.route('/api/activity_enrollment/program')
def activity_enrollment_program():
    # Parse Arguments
    api_key = request.args.get('api_key')

    # Check API Key
    if api_key != API_KEY:
        return jsonify({"message": "Bad API Key!"})

    activity_facts = FactActivityEnrollment.query.\
        join(DimActivity, FactActivityEnrollment.ActivityId == DimActivity.ActivityNumber).\
        with_entities(func.sum(FactActivityEnrollment.Amount), DimActivity.ActivityName, DimActivity.ActivityCategory).\
        group_by(DimActivity.ActivityCategory).\
        all()

    return jsonify(activity_facts=activity_facts)

    return jsonify(total=len(activity_facts), activity_enrollment=[a.serialize() for a in activity_facts])