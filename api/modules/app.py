from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://ras:RaS1p38!BV44jw@rightatschool-test.c6ac6cyneqii.us-east-1.rds.amazonaws.com/rightatschool_testdb'
db = SQLAlchemy(app)

from modules.customer_controller.views import customer
from modules.activity_enrollment_controller.views import activity_enrollment

app.register_blueprint(customer)
app.register_blueprint(activity_enrollment)
