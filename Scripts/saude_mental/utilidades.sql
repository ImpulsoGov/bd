/**************************************************************************

                        SCRIPTS E FUNÇÕES UTILITÁRIAS


 **************************************************************************/


CREATE OR REPLACE FUNCTION 
    saude_mental.classificar_binarios (valor bool)
RETURNS text
LANGUAGE plpgsql
AS $$
    BEGIN
        CASE
            WHEN valor IS NULL THEN RETURN 'Sem informação';
            WHEN valor THEN RETURN 'Sim';
            ELSE RETURN 'Não';
        END CASE;
    END;
$$
;