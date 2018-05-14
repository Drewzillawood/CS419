input_ip_trace = LOAD '/cpre419/ip_trace' USING PigStorage(' ')
AS
(
    time:chararray,
    connectionID:chararray,
    sourceIP:chararray,
    ignore:chararray,
    destinationIP:chararray,
    protocol:chararray,
    protocolData:chararray
);

input_raw_block = LOAD '/cpre419/raw_block' USING PigStorage(' ')
AS
(
    connectionId:chararray,
    actionTaken:chararray
);

raw = FILTER input_raw_block BY actionTaken MATCHES 'Blocked';

joined_input = JOIN input_ip_trace BY connectionID, raw BY connectionId PARALLEL 10;

useful_columns = FOREACH joined_input 
{
    GENERATE time, 
             connectionID, 
             sourceIP, 
             destinationIP, 
             protocol, 
             actionTaken;
};

grouped_sources = GROUP useful_columns BY sourceIP;
counted_sources = FOREACH grouped_sources 
{
    GENERATE $0 AS source,
             COUNT(useful_columns) AS count;
};

STORE counted_sources INTO '/user/drewu/lab6/exp3/output/';