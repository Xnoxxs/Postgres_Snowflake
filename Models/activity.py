
from sqlalchemy import Column, Integer, Text, Boolean, TIMESTAMP, Numeric, ForeignKey, JSON, ARRAY
import datetime
from database import Base

class Activity(Base):
    __tablename__ = 'activities'
    __table_args__ = {'schema': 'Venture'}

    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer, ForeignKey('Venture.owners.id'), nullable=False)

    name = Column(Text, nullable=False)
    description = Column(Text)

    categories = Column(ARRAY(Text))
    collections = Column(ARRAY(Text))
    keywords = Column(ARRAY(Text))

    city = Column(Text)
    country = Column(Text)
    location_name = Column(Text)
    street_address = Column(Text)
    geo_point = Column(JSON)

    verified = Column(Boolean, default=False)
    verification_date = Column(TIMESTAMP)
    created = Column(TIMESTAMP, default=datetime.datetime.now)

    price_amount = Column(Integer)
    price_amount_in_currency = Column(Numeric)
    price_currency = Column(Text)

    type = Column(Text)  # 'event', 'class', etc.
    booking_type = Column(Text)  # 'slots' or 'dateRange'

    details = Column(JSON)
    extra = Column(JSON)
    included = Column(JSON)
    media = Column(JSON)
    split_payment_milestones = Column(JSON)
    cancellation_policies = Column(JSON)
    rental_condition_message = Column(Text)
    allow_split_payment = Column(Boolean, default=False)
