
CREATE OR REPLACE FUNCTION "Venture".sync_to_snowflake()
RETURNS TRIGGER AS $$
BEGIN
    -- This will send a notification; your Python listener will act on it
    PERFORM pg_notify('sync_bookings', '');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;




DROP TRIGGER IF EXISTS booking_sync_trigger ON "Venture".bookings;

CREATE TRIGGER booking_sync_trigger
    AFTER INSERT ON "Venture".bookings
    FOR EACH ROW
    EXECUTE FUNCTION "Venture".sync_to_snowflake();