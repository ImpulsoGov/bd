<!--
SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->

# Scripts para criação de objetos de banco de dados da ImpulsoGov

Extração, tratamento e caregamento de dados públicos direta ou indiretamente relacionados ao Sistema Único de Saúde brasileiro, tendo como destino o banco de dados da [Impulso Gov](https://impulsogov.org/).

*******
## :mag_right: Índice
- [Scripts para criação de objetos de banco de dados da ImpulsoGov](#scripts-para-criação-de-objetos-de-banco-de-dados-da-impulsogov)
  - [:mag_right: Índice](#mag_right-índice)
  - [:milky_way: Estrutura do repositório](#milky_way-estrutura-do-repositório)
  - [:closed_book: Glossário de siglas](#closed_book-glossário-de-siglas)
  - [:registered: Licença](#registered-licença)
*******

*******
  
  
 <div id='estrutura'/>  
 
 ## :milky_way: Estrutura do repositório

O repositório se constitui de um projeto do cliente de banco de dados [Dbeaver](https://dbeaver.io/), que pode ser importado na ferramenta para importação automática de scripts e marcadores com os principais objetos.

```plain
bd
├─ .settings
├─ Scripts
├─ marcadores
```

<div id='glossario'/>  

## :closed_book: Glossário de siglas

| Sigla  | Definição |
| :---    | :----    |
| SCNES    | O [Sistema do Cadastro Nacional de Estabelecimentos de Saúde](https://cnes.datasus.gov.br/pages/estabelecimentos/consulta.jsp) (SCNES) contém informações cadastrais de estabelecimentos, equipes e profissionais de saúde de todo o Brasil.   |
| SIASUS    | O [Sistema de Informações Ambulatoriais SUS](https://cnes.datasus.gov.br/pages/estabelecimentos/consulta.jsp) (SIASUS) é o sistema responsável por receber toda informação dos atendimentos realizados no âmbito ambulatorial do SUS por meio do Boletim de Produção Ambulatorial (BPA) |
| SIHSUS    | O [Sistema de Informações Hospitalares do SUS](https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/) (SIHSUS) reune todos os atendimentos provenientes de internações hospitalares que foram financiados pelo SUS |
| SIM    | O Sistema de Informação Sobre Mortalidade (SIM) armazena dados de vigilância epidemiológica nacional captando informações sobre mortalidade para todas as instâncias do sistema de saúde. |
| SINAN    | O Sistema de Informação de Agravos de Notificação (SINAN) recebe dados de notificação e investigação de casos de doenças e agravos que constam da lista nacional de doenças de notificação compulsória. |
| SISAB    | O Sistema de Informação em Saúde para a Atenção Básica (SISAB) permite consultar informações da Atenção Básica como dados de cadastros, produção, validação da produção para fins de financiamento e de adesão aos programas e estratégias da Política Nacional de Atenção Básica. |
*******

*******
<div id='licenca'/>  

## :registered: Licença
MIT © (?)