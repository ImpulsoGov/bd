CREATE OR REPLACE FUNCTION impulso_previne_dados_nominais.prazo_proximo_dia()
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
  prazo_proximo_dia text;
BEGIN
  prazo_proximo_dia = CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN 'Até 30/Abril'
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN 'Até 31/Agosto'
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN 'Até 31/Dezembro'
                            ELSE NULL::text
                        END;
  RETURN prazo_proximo_dia;
END;
$function$
;
