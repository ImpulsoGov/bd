CREATE OR REPLACE FUNCTION impulso_previne_dados_nominais.equipe_ine(municipio_id_sus text, equipe_ine text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE	
  equipe_ine_unificada text;
  var_municipio_id_sus text;
  var_equipe_ine text;
BEGIN
		var_municipio_id_sus := municipio_id_sus;
		var_equipe_ine := equipe_ine;
	    SELECT tb1.equipe_super_ine INTO equipe_ine_unificada
	    FROM impulso_previne_dados_nominais.codigo_equipe_ine_concebidos tb1
	    WHERE tb1.municipio_id_sus = var_municipio_id_sus AND tb1.equipe_ine = var_equipe_ine;
	    
    -- If equipe_ine_unificada is not null, return it. Otherwise, return var_equipe_ine.
    IF equipe_ine_unificada IS NOT NULL THEN 
        RETURN equipe_ine_unificada;
    ELSE 
        RETURN var_equipe_ine;
    END IF;
END;
$function$
;
