select * from transform_properties;

insert into dim_location(state, city, county, postal_code)
select distinct state, city, county, postal_code from transform_properties;

select * from dim_location;

insert into dim_property(property_type, bedrooms, bathrooms, square_footage, year_built, floor_count)
select distinct property_type, bedrooms, bathrooms, square_footage, year_built, floor_count from transform_properties
on conflict (property_type, bedrooms, bathrooms, square_footage, year_built, floor_count) do nothing;

select * from dim_property;

insert into property_facts(property_facts_id, location_sk, property_sk, full_address, latitude, longitude, legal_description, subdivision)
select tp.id, dl.location_sk, dp.property_sk, tp.full_address, tp.latitude, tp.longitude, tp.legal_description, tp.subdivision from transform_properties tp
join dim_location dl
	on dl.state = tp.state and dl.city = tp.city and dl.postal_code = tp.postal_code
left join dim_property dp
	on dp.property_type = tp.property_type and coalesce(dp.bedrooms,-1) = coalesce(tp.bedrooms,-1)
and coalesce(dp.bathrooms,-1) = coalesce(tp.bathrooms,-1)
on conflict (property_facts_id) do nothing;

select * from property_facts;

insert into dim_occupied(occupied_id, location_sk, property_sk, property_facts_id, owner_occupied, architecture_type)
select tp.id, dl.location_sk, dp.property_sk, pf.property_facts_id, tp.owner_occupied, tp.architecture_type from transform_properties tp
join dim_location dl
	on dl.state = tp.state and dl.city = tp.city and dl.postal_code = tp.postal_code
left join dim_property dp
	on dp.property_type = tp.property_type and coalesce(dp.bedrooms,-1) = coalesce(tp.bedrooms,-1)
and coalesce(dp.bathrooms,-1) = coalesce(tp.bathrooms,-1)
left join property_facts pf
	on pf.full_address = tp.full_address and pf.latitude = tp.latitude and pf.longitude = tp.longitude and pf.legal_description = tp.legal_description
	and pf.subdivision = tp.subdivision
on conflict (occupied_id) do nothing;

select * from dim_occupied;