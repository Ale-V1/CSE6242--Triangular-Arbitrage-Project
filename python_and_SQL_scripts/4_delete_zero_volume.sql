--- rows with zero volume are filler rowswhere there was no actual trading durin those time slots. 
--- We can delete them to save space and improve query performance. 
--- However, since there may be a large number of such rows, 
--- we will delete them in batches to avoid long locks and potential performance issues.
CREATE OR REPLACE PROCEDURE public.delete_volume_zero_in_batches(
    p_batch_size integer DEFAULT 50000,
    p_pause_ms   integer DEFAULT 0
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows_deleted integer;
  v_total_deleted bigint := 0;
BEGIN
  LOOP
    WITH to_del AS (
      SELECT ctid
      FROM public.ohlc_data
      WHERE volume = 0
      LIMIT p_batch_size
      FOR UPDATE SKIP LOCKED
    )
    DELETE FROM public.ohlc_data t
    USING to_del d
    WHERE t.ctid = d.ctid;

    GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;
    v_total_deleted := v_total_deleted + v_rows_deleted;

    RAISE NOTICE 'Deleted % rows this batch (total deleted so far: %).',
                 v_rows_deleted, v_total_deleted;

    IF v_rows_deleted = 0 THEN
      EXIT;
    END IF;

    COMMIT;

    IF p_pause_ms > 0 THEN
      PERFORM pg_sleep(p_pause_ms / 1000.0);
    END IF;

    START TRANSACTION;
  END LOOP;

  COMMIT;
END;
$$;


---------------------------


CALL public.delete_volume_zero_in_batches(200000, 50);
