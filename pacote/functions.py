from zipfile import ZipFile
import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from datetime import datetime
import re
import locale
from selenium import webdriver
import gzip
import json


hoje = datetime.today()
locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

#=========================================================================================================================
def leitura_arquivos(periodo):
#=========================================================================================================================
    """
    Leitura dos arquivos com dados históricos da B3.
      'arquivos/COTAHIST_' + periodo + '.ZIP'

    Returns:
        DataFrame: Um dataframe contendo as colunas necessárias.

    Fonte:
        https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/
    """

    arq_zip = 'arquivos/COTAHIST_' + periodo + '.ZIP'
    arq_txt = 'COTAHIST_' + periodo + '.TXT'

    DTEXCH, CODNEG, PREABE, PREMAX, PREMIN, PREULT, VOLTOT = ([] for i in range(7))

    valores_codbdi = ['02', '07', '08', '12', '13', '14', '34', '35', '36', '58', '96']

    with ZipFile(arq_zip) as myzip:
        with myzip.open(arq_txt) as myfile:
            for line in myfile:
                if (line.decode('utf-8')[0:2] == '01') and (line.decode('utf-8')[10:12] in valores_codbdi):
                    DTEXCH.append(line.decode('utf-8')[2:10])
                    CODNEG.append(line.decode('utf-8')[12:24].rstrip())
                    PREABE.append(int(line.decode('utf-8')[56:69]) / 100)
                    PREMAX.append(int(line.decode('utf-8')[69:82]) / 100)
                    PREMIN.append(int(line.decode('utf-8')[82:95]) / 100)
                    PREULT.append(int(line.decode('utf-8')[108:121]) / 100)
                    VOLTOT.append(int(line.decode('utf-8')[170:188]) / 100)

    df_origem = pd.DataFrame(
        {"Acao": CODNEG
            , "dtPregao": pd.to_datetime(DTEXCH, format="%Y%m%d", errors="ignore")
            , "vrFech": PREULT
            , "vrVolume": VOLTOT
            , "vrMax": PREMAX
            , "vrMin": PREMIN
            , "vrAbert": PREABE
         }
    )

    return df_origem

#=========================================================================================================================
def carrega_dados(arquivos):
#=========================================================================================================================
    """
    Carrega os dados históricos da B3 em dataframe com as marcas de porcentagens alvo atingidas em cada pregão.
      'arquivos/COTAHIST_' + periodo + '.ZIP'

    Returns:
        DataFrame: Um dataframe contendo as colunas necessárias.

    Fonte:
        https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/
    """

    df = leitura_arquivos(arquivos[0])
    for i in range(1, len(arquivos)):
        df = pd.concat([df, leitura_arquivos(arquivos[i])])

    df = df.sort_values(["Acao", "dtPregao"], ascending=True)

    df["pcVar"], df["pcMax"], df["pcMin"], df["pcAbert"] = [
        ((df.vrFech / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrMax / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrMin / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrAbert / df.vrFech.shift(1)) - 1) * 100
    ]

    df["05"], df["10"], df["15"], df["20"], df["25"], df["30"], df["35"], df["40"] = [
        df.apply(condicao05, axis=1)
        , df.apply(condicao10, axis=1)
        , df.apply(condicao15, axis=1)
        , df.apply(condicao20, axis=1)
        , df.apply(condicao25, axis=1)
        , df.apply(condicao30, axis=1)
        , df.apply(condicao35, axis=1)
        , df.apply(condicao40, axis=1)
    ]

    return df

#=========================================================================================================================
def condicao05(df_tmp):
    return 1 if (df_tmp["pcMax"] > 0.5) else 0


def condicao10(df_tmp):
    return 1 if (df_tmp["pcMax"] > 1) else 0


def condicao15(df_tmp):
    return 1 if (df_tmp["pcMax"] > 1.5) else 0


def condicao20(df_tmp):
    return 1 if (df_tmp["pcMax"] > 2) else 0


def condicao25(df_tmp):
    return 1 if (df_tmp["pcMax"] > 2.5) else 0


def condicao30(df_tmp):
    return 1 if (df_tmp["pcMax"] > 3) else 0


def condicao35(df_tmp):
    return 1 if (df_tmp["pcMax"] > 3.5) else 0


def condicao40(df_tmp):
    return 1 if (df_tmp["pcMax"] > 4) else 0


def busca_periodos(df, qt_dias):
    return df.loc[
        df["dtPregao"] >= (df.dtPregao.drop_duplicates().sort_values(ascending=False).iloc[qt_dias - 1])].sort_values(
        ["Acao", "dtPregao"], ascending=False)


def somatorio_pc_max_dia(df_ent, pc, index_name):
    return df_ent.groupby("Acao")["pcMax"].apply(lambda x: (x > pc).sum()).reset_index(name=index_name)


def busca_media(df_ent, coluna, index_name):
    return df_ent.groupby("Acao")[coluna].agg("mean").reset_index(name=index_name)

#=========================================================================================================================
def monta_df_periodos(df_origem, qt_dias):
#=========================================================================================================================
    df_dias = busca_periodos(df_origem, qt_dias)

    df05 = somatorio_pc_max_dia(df_dias, 0.5, "0.5%")
    df10 = somatorio_pc_max_dia(df_dias, 1.0, "resultado")
    df15 = somatorio_pc_max_dia(df_dias, 1.5, "resultado")
    df20 = somatorio_pc_max_dia(df_dias, 2.0, "resultado")
    df25 = somatorio_pc_max_dia(df_dias, 2.5, "resultado")
    df30 = somatorio_pc_max_dia(df_dias, 3.0, "resultado")
    df35 = somatorio_pc_max_dia(df_dias, 3.5, "resultado")
    df40 = somatorio_pc_max_dia(df_dias, 4.0, "resultado")
    df_vol = busca_media(df_dias, "vrVolume", "vol")
    df_vr_fech = busca_media(df_dias, "vrFech", "vrFech")
    df_pc_abert = busca_media(df_dias, "pcAbert", "pcAbert")
    df_pc_soma = df05["0.5%"] + df10["resultado"] + df15["resultado"] + df20["resultado"] + df25["resultado"] + df30[
        "resultado"] + df35["resultado"] + df40["resultado"]

    df05["1.0%"], df05["1.5%"], df05["2.0%"], df05["2.5%"], df05["3.0%"], df05["3.5%"], df05["4.0%"], df05[
        "Soma"], df05["AvgVol"], df05["AvgVrFech"], df05["AvgPcAbert"] = [
        df10["resultado"], df15["resultado"], df20["resultado"], df25["resultado"], df30["resultado"],
        df35["resultado"], df40["resultado"], df_pc_soma, df_vol["vol"], df_vr_fech["vrFech"], df_pc_abert["pcAbert"]]

    df_result = df05.reset_index(drop=True).sort_values(
        ["Soma", "4.0%", "3.5%", "3.0%", "2.5%", "2.0%", "1.5%", "1.0%"],
        ascending=False)

    return df_result

#=========================================================================================================================
def monta_tabela(df_n_dias, vol, col_pc, pc_min, avg_vr_fech, bar):
#=========================================================================================================================
    dados = df_n_dias.loc[
        (df_n_dias["AvgVol"] > vol) & (df_n_dias[col_pc] >= pc_min) & (df_n_dias["AvgVrFech"] > avg_vr_fech)]

    bar.value += 1

    return dados

#=========================================================================================================================
def consulta_acao(df, cd_acao):
#=========================================================================================================================
    df_out = df.copy()
    df_out['vrVolume'] = df['vrVolume'].map('{:,.0f}'.format)
    return df_out.loc[(df_out["Acao"] == cd_acao.upper())].replace(0, "").sort_values(["dtPregao"], ascending=False)


#=========================================================================================================================
def monta_lucro_periodo(df, qt_dias, dias_ant, ic_sort):
#=========================================================================================================================
    qt_dias_full = qt_dias + dias_ant
    df_n_dias = busca_periodos(df, qt_dias_full)

    for i in range(0, dias_ant + 1):
        dt_max = df_n_dias["dtPregao"].max()
        df_n_dias = df_n_dias.loc[df_n_dias["dtPregao"] != dt_max]

    df_n_dias = df_n_dias.loc[df_n_dias["vrFech"] >= 5]

    dt_min = df_n_dias["dtPregao"].min()
    print('\033[94m' + '\033[1m' + f"{dt_min:%Y-%m-%d}" + " >> " + f"{dt_max:%Y-%m-%d}")
    df_dt_min = df_n_dias.loc[(df_n_dias["dtPregao"] == dt_min)].set_index(["Acao"])
    df_dt_max = df_n_dias.loc[(df_n_dias["dtPregao"] == dt_max)].set_index(["Acao"])
    df_avg_vol = busca_media(df_n_dias, "vrVolume", "vol").set_index(["Acao"])

    df_pc_n_dias = pd.DataFrame({
        "dtInicio": df_dt_min["dtPregao"], "dtFim": df_dt_max["dtPregao"]
        , "vrInicio": df_dt_min["vrFech"], "vrFim": df_dt_max["vrFech"]
        , "pcPeriodo": ((df_dt_max["vrFech"] - df_dt_min["vrFech"]) / df_dt_min["vrFech"]) * 100
        , "avgVol": df_avg_vol["vol"]
    })

    df_pc_n_dias = df_pc_n_dias.loc[
        (df_pc_n_dias["avgVol"] > 6000000)].sort_values(["pcPeriodo"], ascending=False) if ic_sort else df_pc_n_dias

    df_pc_n_dias.insert(6, 'posicao', range(1, 1 + len(df_pc_n_dias)))

    return df_pc_n_dias

#=========================================================================================================================
def filtra_data(df, data="max"):
#=========================================================================================================================
    # formato da data: 'aaaa-mm-dd'
    dt_max = df["dtPregao"].max() if data == "max" else datetime.strptime(data, '%Y-%m-%d')
    df_max_dt = df.loc[df["dtPregao"] == dt_max]
    return df_max_dt

#=========================================================================================================================
def verifica_mudanca_vol(df, data="max", multiplier=3):
#=========================================================================================================================
    # data = "2024-04-04"
    df_max_dt = filtra_data(df, data)

    df_2max_dt = df.copy()
    df_2max_dt = df_2max_dt.loc[df_2max_dt["dtPregao"] < df_max_dt.iloc[0, 1]]
    df_2max_dt = filtra_data(df_2max_dt)

    df_max_dt = df_max_dt[["Acao", "vrVolume", "pcVar", "dtPregao", "vrFech"]]
    df_2max_dt = df_2max_dt[["Acao", "vrVolume", "pcVar", "dtPregao", "vrFech"]]

    merge_max = pd.merge(df_2max_dt[['Acao', 'dtPregao', 'vrVolume', 'pcVar', 'vrFech']],
                         df_max_dt[['Acao', 'vrVolume', 'pcVar', 'vrFech', 'dtPregao']], how='inner', on=['Acao'])
    merge_max = merge_max.loc[(merge_max["vrVolume_x"] > 1000000) &
                              (merge_max["vrVolume_y"] > merge_max["vrVolume_x"] * multiplier)]

    merge_max['pcVar_x'] = pd.to_numeric(merge_max['pcVar_x'], errors='coerce')
    merge_max['pcVar_x'] = merge_max['pcVar_x'].apply(lambda x: x * 0.01)
    merge_max['pcVar_x'] = merge_max['pcVar_x'].map('{:.2%}'.format)
    merge_max['pcVar_y'] = pd.to_numeric(merge_max['pcVar_y'], errors='coerce')
    merge_max['pcVar_y'] = merge_max['pcVar_y'].apply(lambda x: x * 0.01)
    merge_max['pcVar_y'] = merge_max['pcVar_y'].map('{:.2%}'.format)

    return merge_max

#=========================================================================================================================
def set_bold(val):
    return "font-weight: bold"


def color_negative_red(val):
    color = 'red' if val < 0 else 'green'
    return 'color: %s' % color

#=========================================================================================================================
def consulta_acao_formatada(df, cd_acao, limite=1000):
#=========================================================================================================================
    acao_temp = consulta_acao(df, cd_acao)[0:limite]
    acao = acao_temp[:-1].copy() if len(acao_temp) > 30 else acao_temp.copy()

    acao.loc[:, 'pcVar'] = pd.to_numeric(acao['pcVar'], errors='coerce')
    acao.loc[:, 'pcVar'] = acao['pcVar'].apply(lambda x: x * 0.01)
    #     acao['pcVar'] = acao['pcVar'].map('{:.2%}'.format)
    acao.loc[:, 'pcMax'] = pd.to_numeric(acao['pcMax'], errors='coerce')
    acao.loc[:, 'pcMax'] = acao['pcMax'].apply(lambda x: x * 0.01)
    # acao['pcMax'] = acao['pcMax'].map('{:.2%}'.format)
    acao.loc[:, 'pcMin'] = pd.to_numeric(acao['pcMin'], errors='coerce')
    acao.loc[:, 'pcMin'] = acao['pcMin'].apply(lambda x: x * 0.01)
    # acao['pcMin'] = acao['pcMin'].map('{:.2%}'.format)
    acao.loc[:, 'pcAbert'] = pd.to_numeric(acao['pcAbert'], errors='coerce')
    acao.loc[:, 'pcAbert'] = acao['pcAbert'].apply(lambda x: x * 0.01)
    # acao['pcAbert'] = acao['pcAbert'].map('{:.2%}'.format)

    acao = acao.replace("nan%", 0)
    # acao.loc[:, 'dtPregao'] = acao['dtPregao'].dt.strftime('%Y-%m-%d')
    acao['dtPregao'] = acao['dtPregao'].dt.strftime('%Y-%m-%d')
    acao = (acao.style.applymap(set_bold, subset=['vrFech', 'pcVar'])
            .applymap(color_negative_red, subset=['pcVar', 'pcMax', 'pcMin', 'pcAbert'])
            .applymap(lambda x: 'color: transparent' if pd.isnull(x) else '')
            )
    acao = acao.format(
        {
            "vrFech": "{:,.2f}".format,
            "vrMax": "{:,.2f}".format,
            "vrMin": "{:,.2f}".format,
            "vrAbert": "{:,.2f}".format,
            "pcVar": "{:,.2%}".format,
            "pcMax": "{:,.2%}".format,
            "pcMin": "{:,.2%}".format,
            "pcAbert": "{:,.2%}".format
        })

    return acao

#=========================================================================================================================
def gera_grafico(list_datas, count1, label1=" ", count2="", label2=" ", count3="", label3=" ", title=" ", set_lim="",
                 figb=3):
#=========================================================================================================================
    fig, ax = plt.subplots(1, figsize=(20, figb))
    if set_lim:
        ax.set_ylim(-30, 30)
    ax.grid()
    fig.autofmt_xdate()
    plt.plot(list(reversed(list_datas)), list(reversed(count1)), label=label1, color="green")
    if count2:
        plt.plot(list(reversed(list_datas)), list(reversed(count2)), label=label2, color="black")
    if count3:
        plt.plot(list(reversed(list_datas)), list(reversed(count3)), label=label3, color="red")
    plt.xticks(list(reversed(list_datas)))
    plt.legend()
    plt.title(title.upper(), fontsize=20)
    plt.show()

#=========================================================================================================================
def grandes_variacoes_volume(df):
#=========================================================================================================================
    vol_var = verifica_mudanca_vol(df, data="max", multiplier=5)
    vol_var["pcVar_y"] = pd.to_numeric(vol_var["pcVar_y"].replace({"%": ""}, regex=True))
    vol_var = vol_var.sort_values(["pcVar_y", "vrVolume_y"], ascending=False)
    vol_var['pcVar_y'] = vol_var['pcVar_y'].apply(lambda x: x * 0.01)
    vol_var['pcVar_y'] = vol_var['pcVar_y'].map('{:.2%}'.format)

    return vol_var if not vol_var.empty else '<< Sem ações com Grandes Variações de Volume >>'

#=========================================================================================================================
# def busca_ativos_dividendos_old():
# #=========================================================================================================================
#     file = "arquivos/agenda_dividendos.html"

#     with open(file, encoding="utf8") as f:
#         dados = f.read()

#     soup = BeautifulSoup(dados, 'html.parser')

#     dict_meses = {
#         'Janeiro': '01',
#         'Fevereiro': '02',
#         'Março': '03',
#         'Abril': '04',
#         'Maio': '05',
#         'Junho': '06',
#         'Julho': '07',
#         'Agosto': '08',
#         'Setembro': '09',
#         'Outubro': '10',
#         'Novembro': '11',
#         'Dezembro': '12'
#     }

#     list_month_group_payment = soup.find_all(attrs={'class': 'month-group-payment'})

#     dic_dividendos = {}
#     set_retorno = {'inicial'}
#     padrao_regex_ano = r'\b\d{4}\b'
#     for month_group in list_month_group_payment:
#         dia = month_group.find(attrs={'class': 'payment-day'}).text
#         mes = month_group.find(attrs={'class': 'text-center'}).text
#         if len(month_group.find_all('h3')) > 0:
#             # ano = month_group.find_all('h3')[0].contents[0][-5:-1]
#             ano = re.findall(padrao_regex_ano, month_group.find_all('h3')[0].contents[0])[0]

#         data = datetime.strptime(f'{dia}-{dict_meses[mes]}-{ano}', '%d-%m-%Y')

#         i = 0
#         dic_dividendos[data] = []
#         for ativo in month_group.find_all('p'):
#             if (i % 3) == 0:
#                 dic_dividendos[data].append(ativo.text)
#                 if data.strftime('%Y-%m-%d') == hoje.strftime('%Y-%m-%d'):
#                     set_retorno.add(ativo.text)
#             i += 1

#     set_retorno.remove('inicial')
#     return set_retorno

#=========================================================================================================================
def busca_ativos_dividendos(dados_div):
#=========================================================================================================================
    """
    Busca ativos que possuem dividendos com 'data com' na data atual.

    Returns:
        dict: Um conjunto contendo os códigos desses ativos.

    Fonte:
        https://investidor10.com.br/acoes/dividendos/2025/marco/
    """

    dic_dt_com = {}
    soup = BeautifulSoup(dados_div, 'html.parser')
    list_ = soup.find_all(attrs={'class': 'hover:bg-gray-50'})

    for acao in list_:
        acao_ticker = acao.find_all('div', class_="ticker-name")[0].text
        data_com = acao.find_all('span', class_="table-field")[0].text
        data = datetime.strptime(data_com, "%d/%m/%y").strftime('%Y-%m-%d')
        if data not in dic_dt_com:
            dic_dt_com[data] = []
        dic_dt_com[data].append(acao_ticker)

    return dic_dt_com

#=========================================================================================================================
def busca_ativos_resultados(dados_rst):
#=========================================================================================================================
    """
    Busca ativos com divulgação de resultados no dia.

    Returns:
        dict: Um conjunto contendo os códigos desses ativos.

    Fonte:
        https://www.moneytimes.com.br/calendario-de-resultados-do-1t25-veja-as-datas-e-horarios-dos-balancos-das-empresas-da-b3-lmrs/
    """

    dic_dt_com = {}
    dados_rst = dados_rst.replace("11/082025", "11/08/2025")
    soup_rst = BeautifulSoup(dados_rst, 'html.parser')

    list_rst = soup_rst.find_all('tr')
    del list_rst[0]

    for ticker in list_rst:
        acao_rst = ticker.find_all('td')[1].text
        data_rst = datetime.strptime(ticker.find_all('td')[2].text[:10], "%d/%m/%Y").strftime('%Y-%m-%d')
        # hr_divlg_rst = ticker.find_all('td')[3].text

        if data_rst not in dic_dt_com:
            dic_dt_com[data_rst] = []
        dic_dt_com[data_rst].append(acao_rst)

    return dic_dt_com

#=========================================================================================================================
def busca_ativos_dividendos_resultados():
#=========================================================================================================================
    hoje_str = hoje.strftime('%Y-%m-%d')
    trimestre_resultados = "2t25"
    url_dividendos = f"https://investidor10.com.br/acoes/dividendos/{hoje.strftime('%Y')}/{hoje.strftime('%B')}/"
    url_resultados = f"https://www.moneytimes.com.br/calendario-de-resultados-do-{trimestre_resultados}-veja-as-datas-e-horarios-dos-balancos-das-empresas-da-b3-lmrs/"

    page_source_list = web_scraping_f([url_dividendos, url_resultados])

    dict_div = busca_ativos_dividendos(page_source_list[0])
    dict_rst = busca_ativos_resultados(page_source_list[1])

    print("\nDividendos:")
    print_rst_div(dict_div)
    # for i, j in dict_div.items():
    #     print(f"{i}: {j}")

    print("\nResultados:")
    print_rst_div(dict_rst)

    final_list = []
    try:
        final_list.extend(dict_div[hoje_str])
    except Exception as e:
        print("sem dividendos")
        print(e)

    try:
        final_list.extend(dict_rst[hoje_str])
    except Exception as e:
        print("sem resultados")

    set_ = set(final_list)

    return set_


def print_rst_div(dict_):
    for i, j in dict_.items():
        if len(j) > 12:
            print(f"{i}:", end="")
            a, b = 0, 0
            fill = " "
            while b < len(j):
                b += 12
                print(f"{fill}{j[a:b]}")
                fill = "            "
                a = b
        else:
            print(f"{i}: {j}")


#=========================================================================================================================
def web_scraping(url):
#=========================================================================================================================
    # initialize an instance of the chrome driver (browser)
    cService = webdriver.ChromeService(executable_path='C:/temp/chromedriver-win64/chromedriver.exe')
    driver = webdriver.Chrome(service=cService)

    # visit your target site
    # driver.get("https://br.investing.com/earnings-calendar/")
    driver.get(url)

    # output the full-page HTML
    codigo_fonte = driver.page_source

    # release the resources allocated by Selenium and shut down the browser
    driver.quit()

    return codigo_fonte

#=========================================================================================================================
def web_scraping_f(url_list):
#=========================================================================================================================
    page_source_list = []

    try:
        cService = webdriver.FirefoxService(executable_path="C:/temp/geckodriver.exe")
        driver = webdriver.Firefox(service=cService)
    except Exception as e:
        driver = webdriver.Firefox()

    for url in url_list:
        print(url)
        driver.get(url)
        page_source = driver.page_source
        page_source_list.append(page_source)

    driver.quit()

    return page_source_list

#=========================================================================================================================
def write_json_gzip(data, jsonfilename):                         # 1. data
#=========================================================================================================================
    '''
    https://stackoverflow.com/questions/39450065/python-3-read-write-compressed-json-objects-from-to-gzip-file
    '''
    json_str = data.to_json(date_format='iso', orient='records') # 2. string (i.e. JSON)
    json_bytes = json_str.encode('utf-8')                        # 3. bytes (i.e. UTF-8)

    with gzip.open(jsonfilename, 'w') as fout:                   # 4. fewer bytes (i.e. gzip)
        fout.write(json_bytes)

#=========================================================================================================================
def read_json_gzip(jsonfilename):
#=========================================================================================================================
    '''
    https://stackoverflow.com/questions/39450065/python-3-read-write-compressed-json-objects-from-to-gzip-file
    '''
    with gzip.open(jsonfilename, 'r') as fin:        # 4. gzip
        json_bytes = fin.read()                      # 3. bytes (i.e. UTF-8)

    json_str = json_bytes.decode('utf-8')            # 2. string (i.e. JSON)
    df_json = pd.read_json(json_str)

    df_json['dtPregao'] = pd.to_datetime(df_json['dtPregao']).dt.strftime('%Y-%m-%d')

    return df_json
