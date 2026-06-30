alter table symbols
drop constraint if exists symbols_code_key;

create unique index if not exists uq_symbols_exchange_code
on symbols (exchange, code);

comment on column klines.source is
'Data source code: 1=seed deterministic sample, 2=pytdx real quote server, 3=tdx_csv local zipped CSV, 0=unknown.';
