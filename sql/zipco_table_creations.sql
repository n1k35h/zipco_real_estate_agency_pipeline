-- SELECT * FROM public.raw_properties
-- SELECT * FROM public.transform_properties
-- drop table raw_properties;
-- drop table transform_properties;
-- drop table dim_location;
-- drop table dim_property;
-- drop table property_facts;
-- drop table dim_occupied;

create table if not exists dim_location (
  location_sk bigserial primary key,
  state text not null,
  city  text not null,
  county text not null,
  postal_code text not null,
  unique(state, city, county, postal_code)
);

create table if not exists dim_property (
	property_sk bigserial not null primary key,
	property_type text,
	bedrooms int,
	bathrooms int,
	square_footage int,
	year_built int,
	floor_count int,
	unique(property_type, bedrooms, bathrooms, square_footage, year_built, floor_count)
);

create table if not exists property_facts (
	property_facts_id text not null primary key,
	location_sk bigint not null references dim_location(location_sk),
	property_sk bigint references dim_property(property_sk),
	full_address text,
	latitude numeric,
	longitude numeric,
	legal_description text,
	subdivision text,
	unique(full_address, latitude, longitude, legal_description, subdivision)
);

create table if not exists dim_occupied (
	occupied_id text not null primary key,
	location_sk bigint not null references dim_location(location_sk),
	property_sk bigint references dim_property(property_sk),
	property_facts_id text references property_facts(property_facts_id),
	owner_occupied boolean,
	architecture_type text
);