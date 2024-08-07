<!--
SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->

# Scripts para criação de objetos de banco de dados da ImpulsoGov

## :mag_right: Pastas
<code>bd_analitico</code>: Registro de códigos do nosso banco analítico.
  - <code>esus-backups</code>: Banco que recebe os dados das transmissões de dados e cria as listas nominais com nossas regras de negócio.
    - <code>configuracoes</code>: Códigos presentes no schema configurações do banco esus-backups. Armazena funções, rotinas, tabelas, views e views materializadas utilizadas no monitoramento das transmissões.
    - <code>impulso_previne_dados_nominais</code>: Códigos presentes no schema impulso_previne_dados_nominais do banco esus-backups.Armazena funçoes, rotinas, tabelas e código das views materializadas utilizadas na construção das nossas listas nominais. Para mais informações leia nossa [wiki](https://github.com/ImpulsoGov/bd/wiki#listas-nominais)
  - <code>principal</code>: Banco que armazena dado dos ETLs de dados públicos.
    - <code>configuracoes</code> : Códigos presentes no schema configurações do banco principal. Códigos das tabelas, funções e rotinas utiliziadas para execução dos nossos processos de ETL.
    - <code>cron</code> : Códigos presentes no schema cron do banco principal. Schema que armazena os agendamentos das rotinas de atualização do nosso banco e sincronização com nosso banco de produção.
    - <code>dados_publicos</code> : Códigos presentes no schema dados_publicos do banco principal. Códigos das funções, tabelas e views utilizadas para armazenar os dados públicos que extraimos através dos nossos ETLs. Para mais informações consulte nosso [repositório de ETLs](https://github.com/ImpulsoGov/etl)
    - <code>impulso_previne</code> : Códigos presentes no schema impulso_previne do banco principal. Códigos utilizados para disponibilização dos dados utilizados na área aberta do ImpulsoPrevine;
    ```plain
        bd_analitico
        ├─ principal
        │  ├─ impulso_previne
        │     ├─ views_materializadas
                  ├─ dados_publicos_area_aberta
        │           └─ ...
        └─ ...
        ```
        
Na pasta <code>dados_publicos_area_aberta</code> encontram-se as seguintes views utilizadas para compor os painéis de dados públicos utilizados na área aberta do Impulso Previne.

<code>codigos_antigos</code>: Códigos não utilizados atualmente

<code>transmissor_impulso_esus</code>: Código do nosso transmissor de dados. Para mais informações leia nossa [wiki](https://github.com/ImpulsoGov/bd/wiki/C%C3%B3digo-do-transmissor)

<code>validacoes_listas_nominais</code>: Códigos para validação após ajustes realizados nas listas nominais.


  

## :registered: Licença
MIT ©
