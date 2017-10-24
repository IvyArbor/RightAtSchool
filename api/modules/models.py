from modules.app import db
from sqlalchemy.inspection import inspect
from datetime import date, datetime

class Serializer(object):

    def serialize(self):
        return {c: getattr(self, c) for c in inspect(self).attrs.keys()}

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]

class DimCustomer(db.Model):
    __tablename__ = "DimCustomer"

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


class DimActivity(db.Model):
    __tablename__ = "DimActivity"

    ActivityId = db.Column(db.Integer(), primary_key=True)
    ActivityCategory = db.Column(db.String())
    ActivityName = db.Column(db.String())
    ActivityNumber = db.Column(db.Integer())
    ActivityStatus = db.Column(db.String())
    ActivityType = db.Column(db.String())
    DaysOfWeek = db.Column(db.String())
    EndDate = db.Column(db.DateTime())
    #EndTime = db.Column(db.String())
    Organization = db.Column(db.String())
    Season = db.Column(db.String())
    Site = db.Column(db.String())
    StartDate = db.Column(db.DateTime())
    #StartTime = db.Column(db.String())
    TransactionDate = db.Column(db.DateTime())
    TransactionType = db.Column(db.String())
    WeekOfMonth = db.Column(db.String())

    def serialize(self):
        return Serializer.serialize(self)


class DimProgram(db.Model):
    __tablename__ = "DimProgram"

    RightClubElectiveNumber = db.Column(db.Integer(), primary_key=True)
    RightClubElectiveName = db.Column(db.String())
    RightClubElectiveType = db.Column(db.String())
    Season = db.Column(db.String())
    RightClubElectiveCategory = db.Column(db.String())
    RightClubElectiveOtherCategory = db.Column(db.String())
    CatalogDescription = db.Column(db.String())
    RegistrationFeeName = db.Column(db.String())
    SiteName = db.Column(db.String())
    LocationDescription = db.Column(db.String())
    DateDescription = db.Column(db.String())
    NumberofMeetingDates = db.Column(db.String())
    NumberofCalendarWeeks = db.Column(db.String())
    MinimumAge = db.Column(db.String())
    MaximumAge = db.Column(db.String())
    AllowWaitingList = db.Column(db.String())
    EnrollmentNotificationEmails = db.Column(db.String())
    WithdrawalNotificationEmails = db.Column(db.String())
    Supervisor = db.Column(db.String())

    def serialize(self):
        return Serializer.serialize(self)


class FactActivityEnrollment(db.Model):
    __tablename__ = "FactActivityEnrollment"

    Id = db.Column(db.Integer(), primary_key=True)
    ActivityId = db.Column(db.Integer())
    CustomerId = db.Column(db.Integer())
    Amount = db.Column(db.String())
    AmountIncTax = db.Column(db.String())
    TotalEnrolled = db.Column(db.String())

    def serialize(self):
        return Serializer.serialize(self)


class FactFlexRegistration(db.Model):
    __tablename__ = "FactFlexRegistration"

    Id = db.Column(db.Integer(), primary_key=True)
    ProgramId = db.Column(db.Integer())
    ProgramLocationId = db.Column(db.String())
    CustomerId = db.Column(db.Integer())
    CustomerLocationId = db.Column(db.String())
    NumberOfEnrolled = db.Column(db.Integer())
    NumberOfHours = db.Column(db.Integer())
    NumberOfClasses = db.Column(db.Integer())

    def serialize(self):
        return Serializer.serialize(self)


class DimCourse(db.Model):
    __tablename__ = "DimCourse"

    Id = db.Column(db.Integer(), primary_key=True)
    Title = db.Column(db.String())
    Score = db.Column(db.Integer())
    Expiration = db.Column(db.String())

    def serialize(self):
        return Serializer.serialize(self)

class DimUser (db.Model):
    __tablename__ = "DimUser"

    Id = db.Column(db.Integer(), primary_key=True)
    FirstName = db.Column(db.String())
    Lastname = db.Column(db.String())
    Email = db.Column(db.String())
    Team = db.Column(db.Text())
    Role = db.Column(db.Text())

    def serialize(self):
        return Serializer.serialize(self)

class FactRecord (db.Model):
    __tablename__ = "FactRecord"

    Id = db.Column(db.Integer(), primary_key=True)
    UserId = db.Column(db.Integer())
    CourseId = db.Column(db.Integer())
    Status = db.Column(db.String())
    AssignmentSource = db.Column(db.String())
    Score = db.Column(db.Integer())
    FinishDate = db.Column(db.DateTime())
    CreateDate = db.Column(db.DateTime())
    ExpirationDate = db.Column(db.DateTime())
    Time = db.Column(db.String())
    CourseLink = db.Column(db.String())
    Link = db.Column(db.String())

    def serialize(self):
        return Serializer.serialize(self)