from modules.app import db
from sqlalchemy.inspection import inspect

class Serializer(object):

    def serialize(self):
        return {c: getattr(self, c) for c in inspect(self).attrs.keys()}

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]

class customer_dimension(db.Model):
    customer_id = db.Column(db.Integer, primary_key=True)
    firstname = db.Column(db.String())
    lastname = db.Column(db.String())
    email = db.Column(db.String())
    Homephone = db.Column(db.String())
    Workphone = db.Column(db.String())
    Cellphone = db.Column(db.String())
    address1 = db.Column(db.String())
    address2 = db.Column(db.String())
    city = db.Column(db.String())
    state = db.Column(db.String())
    zipcode = db.Column(db.String())
    mailingaddress1 = db.Column(db.String())
    mailingaddress2 = db.Column(db.String())
    mailingcity = db.Column(db.String())
    mailingstate = db.Column(db.String())
    mailingzipcode = db.Column(db.String())

    def serialize(self):
        return Serializer.serialize(self)

    def __repr__(self):
        return '<User %r>' % self.firstname


class program_dimension(db.Model):
    program_number = db.Column("Right Club & Elective Number", db.Integer, primary_key=True)
    program_name = db.Column("Right Club & Elective Name", db.String())
    program_type = db.Column("Right Club & Elective Type", db.String())
    season = db.Column("season", db.String())
    program_category = db.Column("Right Club & Elective Category", db.String())
    program_category_other = db.Column("Right Club & Elective Other Category", db.String())
    catalog_description = db.Column("Catalog Description", db.String())

    def serialize(self):
        return Serializer.serialize(self)