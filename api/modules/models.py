from modules.app import db
from sqlalchemy.inspection import inspect

class Serializer(object):

    def serialize(self):
        return {c: getattr(self, c) for c in inspect(self).attrs.keys()}

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]

class DimCustomer(db.Model):
    CustomerId = db.Column(db.Integer, primary_key=True)
    FirstName = db.Column(db.String())
    LastName = db.Column(db.String())
    Email = db.Column(db.String())
    HomePhone = db.Column(db.String())
    WorkPhone = db.Column(db.String())
    CellPhone = db.Column(db.String())
    Address1 = db.Column(db.String())
    Address2 = db.Column(db.String())
    City = db.Column(db.String())
    State = db.Column(db.String())
    Zipcode = db.Column(db.String())
    MailingAddress1 = db.Column(db.String())
    MailingAddress2 = db.Column(db.String())
    MailingCity = db.Column(db.String())
    MailingState = db.Column(db.String())
    MailingZipcode = db.Column(db.String())
    Birthdate = db.Column(db.String())
    GradeId = db.Column(db.String())
    SiteId = db.Column(db.String())
    SiteIdOther = db.Column(db.String())
    CustomertypeId = db.Column(db.String())
    NoMail = db.Column(db.String())
    NoPostalMail = db.Column(db.String())
    Retired = db.Column(db.String())
    MedicalAlert = db.Column(db.String())
    GeneralAlert = db.Column(db.String())
    SpecialHandling = db.Column(db.String())
    Notes = db.Column(db.String())
    EmergencyFName1 = db.Column(db.String())
    EmergencyLName1 = db.Column(db.String())
    EmergencyPhone1 = db.Column(db.String())
    EmergencyRelation1 = db.Column(db.String())
    EmergencyFName2 = db.Column(db.String())
    EmergencyLName2 = db.Column(db.String())
    EmergencyPhone2 = db.Column(db.String())
    EmergencyRelation2 = db.Column(db.String())
    EmergencyOtherPhone1 = db.Column(db.String())
    EmergencyOtherPhone2 = db.Column(db.String())
    AgreeReceiveTextMessage = db.Column(db.String())
    AdditionalEmail = db.Column(db.String())

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