INSERT INTO "nifty_options_selected_data"
SELECT *
FROM "Nifty_options" n
WHERE n."date" BETWEEN n."expiry"::date - INTERVAL '6 days' AND n."expiry"::date
AND NOT EXISTS (
    SELECT 1
    FROM "nifty_options_selected_data" s
    WHERE s.symbol = n.symbol
      AND s.date = n.date
      AND s.expiry = n.expiry
      AND s.strike = n.strike
      AND s.option_type = n.option_type
      AND s.time = n.time
      AND s.open = n.open
      AND s.high = n.high
      AND s.low = n.low
      AND s.close = n.close
      AND s.volume = n.volume
      AND s.oi = n.oi
      AND s.option_nm = n.option_nm
);