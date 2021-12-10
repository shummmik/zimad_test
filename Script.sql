select avg(ARPDAU)
	from
	(select
	event_time::date as event_date,
	sum(event_value) FILTER(WHERE event_name = 'purchase')/
	count(distinct user_id) FILTER(WHERE event_name = 'launch') as ARPDAU
	from
	public.test
	group by event_date
	order by event_date) t