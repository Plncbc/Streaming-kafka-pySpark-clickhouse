create table IF NOT EXISTS from_pySpark(
	event_type FixedString(8),
	event_time DateTime,
	agg_trade_id UInt32,
	traded_pair FixedString(8),
	price Float32,
	quantity Int32,
	first_trade_id UInt32,
	last_trade_id UInt32,
	trade_time DateTime,
	is_market_maker Boolean,
	trade_value Float32,
	delay_ms Int32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
Order by agg_trade_id

drop table from_pySpark 

drop VIEW market_analysis_mv


create table IF NOT EXISTS table_for_mview(
	agg_time DateTime,
	RSI Float32,
	curr_coin_state_at_the_end_of_time_period Float32
)
ENGINE = ReplacingMergeTree
Order by agg_time



CREATE MATERIALIZED VIEW IF NOT EXISTS materialized_view
to table_for_mview
AS
select
    toStartOfInterval(event_time, INTERVAL 14 day) as agg_time,
    round(avg(100 - 100 / (1 + avg_gain / avg_loss)), 5) as RSI,
    round(last_value(price) - first_value(price), 5) as curr_coin_state_at_the_end_of_time_period
from (
		select 
			traded_pair,
			curr.agg_trade_id,
		    event_time,
		    curr.price,
		    avgIf(curr.price - prev.price, curr.price - prev.price > 0) OVER (PARTITION BY traded_pair ORDER BY event_time) AS avg_gain,
		   	avgIf(abs(curr.price - prev.price), curr.price - prev.price < 0) OVER (PARTITION BY traded_pair ORDER BY event_time) AS avg_loss
		FROM from_pySpark curr
		join from_pySpark prev 
			on curr.agg_trade_id = prev.agg_trade_id + 1
		WHERE event_time BETWEEN now() - INTERVAL 14 day AND now() + INTERVAL 1 day
	)
where not(isNaN(avg_gain) or isNaN(avg_loss))
group by agg_time


SELECT *
from materialized_view

SELECT *
FROM system.query_log
