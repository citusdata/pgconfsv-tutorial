CREATE OR REPLACE FUNCTION update_daily_aggregates()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
	LOOP
		UPDATE data_daily_counts SET value = value+1 WHERE created_date = NEW.created_at::date;
		IF found THEN
			RETURN NULL;
		END IF;
		BEGIN
			INSERT INTO data_daily_counts VALUES (NEW.created_at::date, 1);
			RETURN NULL;	
		EXCEPTION WHEN unique_violation THEN
      		-- retry
		END;
	END LOOP;
END;
$body$;

CREATE TRIGGER data_change
AFTER INSERT ON data
FOR EACH ROW
EXECUTE PROCEDURE update_daily_aggregates();
