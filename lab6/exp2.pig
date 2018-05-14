-- Retrieve input data
input_lines = LOAD '/cpre419/network_trace' USING PigStorage(' ') 
AS 
(
  time:chararray,
  e_1:chararray, 
  sourceIp:chararray, 
  e_2:chararray, 
  destinationIp:chararray, 
  protocol:chararray, 
  protocolDepData:chararray
);

-- Only keep the columns that are useful
useful_columns = FOREACH input_lines
{
    GENERATE REGEX_EXTRACT(sourceIp, '(^(([0-9]*).){3}[0-9]*)', 0) AS source,
             REGEX_EXTRACT(destinationIp, '(^(([0-9]*).){3}[0-9]*)', 0) AS dest,
             protocol;
};

-- Only want tcp data
filtered_tcp = FILTER useful_columns BY protocol MATCHES 'tcp';

-- Gather non-duplicate destination addresses
unique_dest = FOREACH (GROUP filtered_tcp BY source)
{
    this_dest = filtered_tcp.dest;
    unique    = DISTINCT this_dest;
    GENERATE  $0 AS source,
              FLATTEN(unique) AS dest;
};

-- Do some sorting so it looks nice
group_sources = GROUP unique_dest BY source;
group_counts  = FOREACH group_sources 
{
    GENERATE COUNT(unique_dest) AS count, 
             $0 AS source;
};
top_ten = ORDER group_counts BY count DESC;

-- Output 
STORE top_ten INTO '/user/drewu/lab6/exp2/output/';