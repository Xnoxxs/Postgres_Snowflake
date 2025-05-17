
from sqlalchemy import Column, Integer, Text, Boolean, TIMESTAMP, Numeric, ForeignKey, JSON
from database import Base

class Booking(Base):
    __tablename__ = 'bookings'
    __table_args__ = {'schema': 'Venture'}

    id = Column(Integer, primary_key=True)

    activity_id = Column(Integer, ForeignKey('Venture.activities.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('Venture.users.id'), nullable=False)

    people = Column(Integer)
    extras = Column(JSON)

    promotion = Column(Boolean)
    promotion_id = Column(Text, nullable=True)

    status = Column(Text)
    booking_type = Column(Text)

    start_date = Column(TIMESTAMP)
    end_date = Column(TIMESTAMP)
    reservation_date = Column(TIMESTAMP)

    activity_price = Column(Numeric)
    total_price = Column(Numeric)

    asked_for_rating = Column(Boolean, default=False)
    is_rated = Column(Boolean, default=False)

    cancellation_policies = Column(JSON)  # List of { hours, refund_percentage }
    refund = Column(JSON)  # Object with refund fields

    payment_intent_id = Column(Text)

