from zipfile import ZipFile
import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from datetime import datetime


def leitura_arquivos(periodo):
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
        {"cdAcao": CODNEG
            , "dtPregao": pd.to_datetime(DTEXCH, format="%Y%m%d", errors="ignore")
            , "vrFech": PREULT
            , "vrVolume": VOLTOT
            , "vrMaxDia": PREMAX
            , "vrMinDia": PREMIN
            , "vrAbert": PREABE
         }
    )

    return df_origem


def carrega_dados(arquivos):
    df = leitura_arquivos(arquivos[0])
    for i in range(1, len(arquivos)):
        df = pd.concat([df, leitura_arquivos(arquivos[i])])

    df = df.sort_values(["cdAcao", "dtPregao"], ascending=True)

    df["pcVar"], df["pcMaxDia"], df["pcMinDia"], df["pcAbert"] = [
        ((df.vrFech / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrMaxDia / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrMinDia / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrAbert / df.vrFech.shift(1)) - 1) * 100
    ]

    df["i05"], df["i10"], df["i15"], df["i20"], df["i25"], df["i30"] = [
        df.apply(condicao05, axis=1)
        , df.apply(condicao10, axis=1)
        , df.apply(condicao15, axis=1)
        , df.apply(condicao20, axis=1)
        , df.apply(condicao25, axis=1)
        , df.apply(condicao30, axis=1)
    ]

    return df


def condicao05(df_tmp):
    return 1 if (df_tmp["pcMaxDia"] > 0.5) else 0


def condicao10(df_tmp):
    return 1 if (df_tmp["pcMaxDia"] > 1) else 0


def condicao15(df_tmp):
    return 1 if (df_tmp["pcMaxDia"] > 1.5) else 0


def condicao20(df_tmp):
    return 1 if (df_tmp["pcMaxDia"] > 2) else 0


def condicao25(df_tmp):
    return 1 if (df_tmp["pcMaxDia"] > 2.5) else 0


def condicao30(df_tmp):
    return 1 if (df_tmp["pcMaxDia"] > 3) else 0


def busca_periodos(df, qt_dias):
    return df.loc[
        df["dtPregao"] >= (df.dtPregao.drop_duplicates().sort_values(ascending=False).iloc[qt_dias - 1])].sort_values(
        ["cdAcao", "dtPregao"], ascending=False)


def somatorio_pc_max_dia(df_ent, pc, index_name):
    return df_ent.groupby("cdAcao")["pcMaxDia"].apply(lambda x: (x > pc).sum()).reset_index(name=index_name)


def busca_media(df_ent, coluna, index_name):
    return df_ent.groupby("cdAcao")[coluna].agg("mean").reset_index(name=index_name)


def monta_df_periodos(df_origem, qt_dias):
    df_dias = busca_periodos(df_origem, qt_dias)

    df05 = somatorio_pc_max_dia(df_dias, 0.5, "0.5%")
    df10 = somatorio_pc_max_dia(df_dias, 1.0, "resultado")
    df15 = somatorio_pc_max_dia(df_dias, 1.5, "resultado")
    df20 = somatorio_pc_max_dia(df_dias, 2.0, "resultado")
    df25 = somatorio_pc_max_dia(df_dias, 2.5, "resultado")
    df30 = somatorio_pc_max_dia(df_dias, 3.0, "resultado")
    df_vol = busca_media(df_dias, "vrVolume", "vol")
    df_vr_fech = busca_media(df_dias, "vrFech", "vrFech")
    df_pc_abert = busca_media(df_dias, "pcAbert", "pcAbert")
    df_pc_soma = df05["0.5%"] + df10["resultado"] + df15["resultado"] + df20["resultado"] + df25["resultado"] + df30[
        "resultado"]

    df05["1.0%"], df05["1.5%"], df05["2.0%"], df05["2.5%"], df05["3.0%"], df05["Soma"], df05["AvgVol"], df05[
        "AvgVrFech"], df05["AvgPcAbert"] = [
        df10["resultado"], df15["resultado"], df20["resultado"], df25["resultado"], df30["resultado"], df_pc_soma,
        df_vol["vol"], df_vr_fech["vrFech"], df_pc_abert["pcAbert"]]

    df_result = df05.reset_index(drop=True).sort_values(["Soma", "3.0%", "2.5%", "2.0%", "1.5%", "1.0%"],
                                                        ascending=False)

    return df_result


def monta_tabela(df_n_dias, vol, col_pc, pc_min, avg_vr_fech, bar):
    dados = df_n_dias.loc[
        (df_n_dias["AvgVol"] > vol) & (df_n_dias[col_pc] >= pc_min) & (df_n_dias["AvgVrFech"] > avg_vr_fech)]

    bar.value += 1

    return dados


def consulta_acao(df, cd_acao):
    df_out = df.copy()
    df_out['vrVolume'] = df['vrVolume'].map('{:,.0f}'.format)
    return df_out.loc[(df_out["cdAcao"] == cd_acao.upper())].replace(0, "").sort_values(["dtPregao"], ascending=False)


def monta_lucro_periodo(df, qt_dias, dias_ant, ic_sort):
    qt_dias_full = qt_dias + dias_ant
    df_n_dias = busca_periodos(df, qt_dias_full)

    for i in range(0, dias_ant + 1):
        dt_max = df_n_dias["dtPregao"].max()
        df_n_dias = df_n_dias.loc[df_n_dias["dtPregao"] != dt_max]

    df_n_dias = df_n_dias.loc[df_n_dias["vrFech"] >= 5]

    dt_min = df_n_dias["dtPregao"].min()
    print('\033[94m' + '\033[1m' + f"{dt_min:%Y-%m-%d}" + " >> " + f"{dt_max:%Y-%m-%d}")
    df_dt_min = df_n_dias.loc[(df_n_dias["dtPregao"] == dt_min)].set_index(["cdAcao"])
    df_dt_max = df_n_dias.loc[(df_n_dias["dtPregao"] == dt_max)].set_index(["cdAcao"])
    df_avg_vol = busca_media(df_n_dias, "vrVolume", "vol").set_index(["cdAcao"])

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


def filtra_data(df, data="max"):
    # formato da data: 'aaaa-mm-dd'
    dt_max = df["dtPregao"].max() if data == "max" else datetime.strptime(data, '%Y-%m-%d')
    df_max_dt = df.loc[df["dtPregao"] == dt_max]
    return df_max_dt


def verifica_mudanca_vol(df, data="max", multiplier=3):
    # data = "2024-04-04"
    df_max_dt = filtra_data(df, data)

    df_2max_dt = df.copy()
    df_2max_dt = df_2max_dt.loc[df_2max_dt["dtPregao"] < df_max_dt.iloc[0, 1]]
    df_2max_dt = filtra_data(df_2max_dt)

    df_max_dt = df_max_dt[["cdAcao", "vrVolume", "pcVar", "dtPregao", "vrFech"]]
    df_2max_dt = df_2max_dt[["cdAcao", "vrVolume", "pcVar", "dtPregao", "vrFech"]]

    merge_max = pd.merge(df_2max_dt[['cdAcao', 'dtPregao', 'vrVolume', 'pcVar', 'vrFech']],
                         df_max_dt[['cdAcao', 'vrVolume', 'pcVar', 'vrFech', 'dtPregao']], how='inner', on=['cdAcao'])
    merge_max = merge_max.loc[(merge_max["vrVolume_x"] > 1000000) &
                              (merge_max["vrVolume_y"] > merge_max["vrVolume_x"] * multiplier)]

    merge_max['pcVar_x'] = pd.to_numeric(merge_max['pcVar_x'], errors='coerce')
    merge_max['pcVar_x'] = merge_max['pcVar_x'].apply(lambda x: x * 0.01)
    merge_max['pcVar_x'] = merge_max['pcVar_x'].map('{:.2%}'.format)
    merge_max['pcVar_y'] = pd.to_numeric(merge_max['pcVar_y'], errors='coerce')
    merge_max['pcVar_y'] = merge_max['pcVar_y'].apply(lambda x: x * 0.01)
    merge_max['pcVar_y'] = merge_max['pcVar_y'].map('{:.2%}'.format)

    return merge_max


def consulta_acao_formatada(df, cd_acao):
    acao = consulta_acao(df, cd_acao)

    acao['pcVar'] = pd.to_numeric(acao['pcVar'], errors='coerce')
    acao['pcVar'] = acao['pcVar'].apply(lambda x: x * 0.01)
    acao['pcVar'] = acao['pcVar'].map('{:.2%}'.format)
    acao['pcMaxDia'] = pd.to_numeric(acao['pcMaxDia'], errors='coerce')
    acao['pcMaxDia'] = acao['pcMaxDia'].apply(lambda x: x * 0.01)
    acao['pcMaxDia'] = acao['pcMaxDia'].map('{:.2%}'.format)
    acao['pcMinDia'] = pd.to_numeric(acao['pcMinDia'], errors='coerce')
    acao['pcMinDia'] = acao['pcMinDia'].apply(lambda x: x * 0.01)
    acao['pcMinDia'] = acao['pcMinDia'].map('{:.2%}'.format)
    acao['pcAbert'] = pd.to_numeric(acao['pcAbert'], errors='coerce')
    acao['pcAbert'] = acao['pcAbert'].apply(lambda x: x * 0.01)
    acao['pcAbert'] = acao['pcAbert'].map('{:.2%}'.format)

    acao = acao.replace("nan%", 0)

    return acao


def gera_grafico(list_datas, count):
    fig, ax = plt.subplots(1, figsize=(20, 3))
    ax.grid()
    fig.autofmt_xdate()
    plt.plot(list(reversed(list_datas)), list(reversed(count)))
    plt.show()


def grandes_variacoes_volume(df):
    vol_var = verifica_mudanca_vol(df, data="max", multiplier=5)
    vol_var["pcVar_y"] = pd.to_numeric(vol_var["pcVar_y"].replace({"%": ""}, regex=True))
    vol_var = vol_var.sort_values(["pcVar_y", "vrVolume_y"], ascending=False)
    vol_var['pcVar_y'] = vol_var['pcVar_y'].apply(lambda x: x * 0.01)
    vol_var['pcVar_y'] = vol_var['pcVar_y'].map('{:.2%}'.format)

    return vol_var


def busca_ativos_dividendos():
    file = "agenda_dividendos_clean.html"

    with open(file) as f:
        dados = f.read()

    soup = BeautifulSoup(dados, 'html.parser')

    dict_meses = {
        'Janeiro': '01',
        'Fevereiro': '02',
        'MarÃ§o': '03',
        'Abril': '04',
        'Maio': '05',
        'Junho': '06',
        'Julho': '07',
        'Agosto': '08',
        'Setembro': '09',
        'Outubro': '10',
        'Novembro': '11',
        'Dezembro': '12'
    }

    list_month_group_payment = soup.find_all(attrs={'class': 'month-group-payment'})

    hoje = datetime.today()
    dic_dividendos = {}
    lista_retorno = []
    for month_group in list_month_group_payment:
        dia = month_group.find(attrs={'class': 'payment-day'}).text
        mes = month_group.find(attrs={'class': 'text-center'}).text
        if len(month_group.find_all('h3')) > 0:
            ano = month_group.find_all('h3')[0].contents[0][-5:-1]

        data = datetime.strptime(f'{dia}-{dict_meses[mes]}-{ano}', '%d-%m-%Y')

        i = 0
        dic_dividendos[data] = []
        for ativo in month_group.find_all('p'):
            if (i % 3) == 0:
                dic_dividendos[data].append(ativo.text)
                if data.strftime('%Y-%m-%d') == hoje.strftime('%Y-%m-%d'):
                    lista_retorno.append(ativo.text)
            i += 1

    return lista_retorno
