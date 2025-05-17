from database import Session
from Models.booking import Booking
from Models.activity import Activity
from Models.user import User
import datetime

def add_booking(
    activity_id,
    user_id,
    people,
    extras,
    promotion,
    promotion_id,
    status,
    booking_type,
    start_date,
    end_date,
    reservation_date,
    activity_price,
    total_price,
    asked_for_rating,
    is_rated,
    cancellation_policies,
    refund,
    payment_intent_id
):
    session = Session()
    try:
        new_booking = Booking(
            activity_id=activity_id,
            user_id=user_id,
            people=people,
            extras=extras,
            promotion=promotion,
            promotion_id=promotion_id,
            status=status,
            booking_type=booking_type,
            start_date=start_date,
            end_date=end_date,
            reservation_date=reservation_date,
            activity_price=activity_price,
            total_price=total_price,
            asked_for_rating=asked_for_rating,
            is_rated=is_rated,
            cancellation_policies=cancellation_policies,
            refund=refund,
            payment_intent_id=payment_intent_id
        )
        session.add(new_booking)
        session.commit()
        print("Booking added successfully!")
    except Exception as e:
        print("Error adding booking:", e)
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Example usage
    add_booking(
        activity_id=1,
        user_id=1,
        people=2,
        extras={"Camera Rental": 1000},
        promotion=True,
        promotion_id="PROMO2024",
        status="confirmed",
        booking_type="slots",
        start_date=datetime.datetime(2025, 6, 1, 10, 0, 0),
        end_date=datetime.datetime(2025, 6, 1, 12, 0, 0),
        reservation_date=datetime.datetime.now(),
        activity_price=50000,
        total_price=50000,
        asked_for_rating=False,
        is_rated=False,
        cancellation_policies=[{"hours": 48, "refund_percentage": 50}],
        refund={"is_refunded": False},
        payment_intent_id="pi_new_123"
    )