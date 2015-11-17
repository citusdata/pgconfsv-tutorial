CREATE OR REPLACE FUNCTION update_daily_aggregates()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
	LOOP
		UPDATE data_daily_counts_cached SET value = value+1 WHERE created_date = NEW.created_at::date;
		IF found THEN
			RETURN NULL;
		END IF;
		BEGIN
			INSERT INTO data_daily_counts_cached VALUES (NEW.created_at::date, 1);
			RETURN NULL;	
		EXCEPTION WHEN unique_violation THEN
      		-- retry
		END;
	END LOOP;
END;
$body$;

DROP TRIGGER IF EXISTS data_change ON data;

CREATE TRIGGER data_change
AFTER INSERT ON data
FOR EACH ROW
EXECUTE PROCEDURE update_daily_aggregates();
