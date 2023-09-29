CREATE OR REPLACE FUNCTION configuracoes.limpar_wal_antigo()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    max_retention_days integer := 7;  -- Exemplo: manter por 7 dias
BEGIN
    -- Exclua registros de WAL antigos
    PERFORM pg_switch_wal();
    EXECUTE 'SELECT pg_archivecleanup(' || quote_literal(pg_current_wal_lsn()) || ', true)';
END;
$function$
;
