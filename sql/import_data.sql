COPY {table_name} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');
ANALYZE {table_name};