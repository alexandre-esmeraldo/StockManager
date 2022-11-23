from zipfile import ZipFile
import pandas as pd


def leituraArquivos(periodo):
    arq_zip = 'arquivos/COTAHIST_' + periodo + '.ZIP'
    arq_txt = 'COTAHIST_' + periodo + '.TXT'

    DTEXCH, CODNEG, PREABE, PREMAX, PREMIN, PREULT, VOLTOT = ([] for i in range(7))

    with ZipFile(arq_zip) as myzip:
        with myzip.open(arq_txt) as myfile:
            for line in myfile:
                if (line.decode('utf-8')[0:2] == '01') and (line.decode('utf-8')[10:12] == '02'):
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
    ).sort_values(["cdAcao", "dtPregao"], ascending=True)

    return df_origem


def carrega_dados(arquivos):
    df = leituraArquivos(arquivos[0])

    for i in range(1, len(arquivos)):
        df = pd.concat([df, leituraArquivos(arquivos[i])])

    #    df = pd.concat([df_112021, df_122021, df])

    df["pcVar"], df["pcMaxDia"], df["pcMinDia"], df["pcAbert"] = [
        ((df.vrFech / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrMaxDia / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrMinDia / df.vrFech.shift(1)) - 1) * 100
        , ((df.vrAbert / df.vrFech.shift(1)) - 1) * 100
    ]

    df["ic05"], df["ic10"], df["ic15"], df["ic20"], df["ic25"], df["ic30"] = [
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


def buscaPeriodos(df, qt_dias):
    return df.loc[
        df["dtPregao"] >= (df.dtPregao.drop_duplicates().sort_values(ascending=False).iloc[qt_dias])].sort_values(
        ["cdAcao", "dtPregao"], ascending=False)


def somatorioPcMaxDia(df_ent, pc, index_name):
    return df_ent.groupby("cdAcao")["pcMaxDia"].apply(lambda x: (x > pc).sum()).reset_index(name=index_name)


def buscaMedia(df_ent, coluna, index_name):
    return df_ent.groupby("cdAcao")[coluna].agg("mean").reset_index(name=index_name)


def montaDfPeriodos(df_origem, qt_dias):
    df_dias = buscaPeriodos(df_origem, qt_dias - 1)

    df05 = somatorioPcMaxDia(df_dias, 0.5, "0.5%")
    df10 = somatorioPcMaxDia(df_dias, 1.0, "resultado")
    df15 = somatorioPcMaxDia(df_dias, 1.5, "resultado")
    df20 = somatorioPcMaxDia(df_dias, 2.0, "resultado")
    df25 = somatorioPcMaxDia(df_dias, 2.5, "resultado")
    df30 = somatorioPcMaxDia(df_dias, 3.0, "resultado")
    dfVol = buscaMedia(df_dias, "vrVolume", "vol")
    dfVrFech = buscaMedia(df_dias, "vrFech", "vrFech")
    dfPcAbert = buscaMedia(df_dias, "pcAbert", "pcAbert")

    df05["1.0%"], df05["1.5%"], df05["2.0%"], df05["2.5%"], df05["3.0%"], df05["AvgVol"], df05["AvgVrFech"], df05[
        "AvgPcAbert"] = [
        df10["resultado"], df15["resultado"], df20["resultado"], df25["resultado"], df30["resultado"], dfVol["vol"],
        dfVrFech["vrFech"], dfPcAbert["pcAbert"]]

    df_result = df05.reset_index(drop=True).sort_values(["1.0%", "1.5%", "2.0%", "2.5%", "3.0%"],
                                                        ascending=False)

    return df_result


def montaTabela(df_n_dias, vol, col_pc, pc_min, avg_vr_fech):
    return df_n_dias.loc[
        (df_n_dias["AvgVol"] > vol) & (df_n_dias[col_pc] >= pc_min) & (df_n_dias["AvgVrFech"] > avg_vr_fech)]


def consultaAcao(df, cd_acao):
    return df.loc[(df["cdAcao"] == cd_acao)].replace(0, "").sort_values(["dtPregao"], ascending=False)
