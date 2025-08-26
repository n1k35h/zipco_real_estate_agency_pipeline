-- Count the number of each cities appears in the current listing of properties:
select state, city, count(city) as total_number_of_cities_in_current_property_listing from dim_location
group by state, city
order by "total_number_of_cities_in_current_property_listing" desc;

-- Count the number of each property_ypes appears in the current listing of properties:
select property_type, count(property_type) as how_many_different_types_of_property_in_the_current_listing from dim_property
where property_type is not null
group by property_type
order by "how_many_different_types_of_property_in_the_current_listing" desc;

-- Count the property types in each city?
select dl.city, dp.property_type, count(dp.property_type) as property_type_count
from property_facts pf
join dim_location dl on pf.location_sk = dl.location_sk
join dim_property dp on pf.property_sk = dp.property_sk
where dp.property_type is not null
group by dl.city, dp.property_type
order by city asc;

-- Which properties has legal description?
select pf.full_address, pf.legal_description, pf.subdivision 
from property_facts pf
where pf.legal_description is not null;

-- Which property is occupied?
select pf.full_address, dl.city, doc.owner_occupied from dim_occupied doc
join property_facts pf on doc.property_facts_id = pf.property_facts_id
join dim_location dl
where doc.owner_occupied = TRUE;
