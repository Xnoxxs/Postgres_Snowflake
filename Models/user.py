

from sqlalchemy import Column, Integer, Text, Boolean, TIMESTAMP, ARRAY
import datetime
from database import Base

class User(Base):
    __tablename__ = 'users'
    __table_args__ = {'schema': 'Venture'}

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    email = Column(Text, unique=True, nullable=False)
    password = Column(Text, nullable=False)
    created = Column(TIMESTAMP, default=datetime.datetime.now)

    customer_id = Column(Text)
    customer_created = Column(TIMESTAMP)
    payment_method_default = Column(Text)

    card_brand = Column(Text)
    card_payment_method_id = Column(Text)
    card_last4 = Column(Integer)
    card_expiry_month = Column(Integer)
    card_expiry_year = Column(Integer)

    last_logged_in = Column(TIMESTAMP)
    last_logged_out = Column(TIMESTAMP)

    photo = Column(Text)
    push_notifications_permission = Column(Boolean, default=True)
    owner = Column(Boolean, default=False)

    favourites = Column(ARRAY(Integer))
    devices_tokens = Column(ARRAY(Text))
