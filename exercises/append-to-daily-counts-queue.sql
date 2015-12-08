CREATE OR REPLACE FUNCTION append_to_daily_counts_queue()
RETURNS TRIGGER LANGUAGE plpgsql
AS $body$
BEGIN
    CASE TG_OP
    WHEN 'INSERT' THEN
        INSERT INTO data_daily_counts_queue(created_date, diff)
        VALUES (date_trunc('day', NEW.created_at), +1);

    WHEN 'UPDATE' THEN
        INSERT INTO data_daily_counts_queue(created_date, diff)
        VALUES (date_trunc('day', NEW.created_at), +1);

        INSERT INTO data_daily_counts_queue(created_date, diff)
        VALUES (date_trunc('day', OLD.created_at), -1);

    WHEN 'DELETE' THEN

        INSERT INTO data_daily_counts_queue(created_date, diff)
        VALUES (date_trunc('day', OLD.created_at), -1);

    END CASE;

    IF random() < 0.0001 THEN  /* 1/10,000 probability */
       PERFORM flush_daily_counts_queue();
    END IF;

    RETURN NULL;
END;
$body$;

DROP TRIGGER IF EXISTS data_change ON data;
CREATE TRIGGER data_change after 
INSERT OR UPDATE OR DELETE 
ON data FOR each row
EXECUTE PROCEDURE append_to_daily_counts_queue();
