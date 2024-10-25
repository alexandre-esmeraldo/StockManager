# StockManager

Olá, bem-vindo.

Esse é um modelo analítico que analisa o histórico de comportamento de ações da Bovespa na tentativa de prever qual ação tem maior probabilidade de atingir 1% de valorização no pregão seguinte, baseado no valor de fechamento.

É necessário fazer download do arquivo de séries históricas da B3 para a pasta arquivos/. Os dados de dois meses é suficiente, e geralmente apenas o arquivo anual atende.

O arquivo possui nomes padronizados, como COTAHIST_A2023.ZIP (anual), COTAHIST_D16112022.ZIP (diário), COTAHIST_M112022.ZIP (mensal).

Basta adicionar os arquivos desejados na lista LISTA_ARQUIVOS no notebook antes da execução, e somente a parte variável do nome é necessária (A2023, D16112022 e/ou M112022, conforme exemplos acima). Não há limite de arquivos.
