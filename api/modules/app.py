from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://ras:RaS1p38!BV44jw@rightatschool-test.c6ac6cyneqii.us-east-1.rds.amazonaws.com/rightatschool_testdb'
db = SQLAlchemy(app)


from modules.models import *
@app.route("/customers")
def customers():
    #customers = customer_dimension.query.with_entities(customer_dimension.firstname, customer_dimension.lastname).all()
    customers = customer_dimension.query.all()

    return jsonify(total=len(customers), customers=[c.serialize() for c in customers])


@app.route("/programs")
def programs():
    programs = program_dimension.query.all()

    return jsonify(total=len(programs), programs=[p.serialize() for p in programs])
