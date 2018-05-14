input_lines = LOAD 'gaz_tracts_national.txt' USING PigStorage('\t')
AS (USPS:chararray, GEOID:int, POP10:int, HU10:int, ALAND:long, AWATER:int, ALAND_SQMI:double, AWATER_SQMI:double, INTPTLAT:float, INTPTLONG:float);

ranked = RANK input_lines;

no_header = FILTER ranked BY (rank_input_lines > 1);

land_areas = FOREACH no_header GENERATE USPS,ALAND;

states = GROUP land_areas BY USPS;

totaled_land_areas = FOREACH states GENERATE $0 AS USPS, SUM(land_areas.ALAND) AS SUM_LAND_AREAS;

ordered_max = ORDER totaled_land_areas BY SUM_LAND_AREAS DESC;

top_ten = FILTER ordered_max BY (SUM_LAND_AREAS > 200000000000L);

STORE top_ten INTO 'output';