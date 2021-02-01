# Carregar dados históricos da Bovespa em tabela para análise

import sqlite3 as lite
import sys

""""
-- Ações que mais bateram 1% nos últimos x dias
select cd_acao, sum(ic_1_00_pc), sum(ic_1_50_pc), sum(ic_2_00_pc), sum(vr_volume), sum(vr_result_1_00_pc), sum(vr_result_1_50_pc), sum(vr_result_2_00_pc) from hist_dados
where dt_pregao in (select distinct dt_pregao 
     from hist_dados order by 1 DESC limit 15)
group by cd_acao having sum(ic_1_00_pc) >= 13 order by 5 DESC

-- Detalhes de uma ação específica
SELECT cd_acao, dt_pregao, vr_fechamento as cotacao, vr_volume, pc_variacao as percent, vr_maximo_dia as vr_max, vr_minimo_dia as vr_min, pc_maximo_dia as pc_max, pc_minimo_dia as pc_min, ">", pc_abertura as abert, "<", case ic_1_00_pc when 1 then 'sim' else '' end _1_00, case ic_1_50_pc when 1 then 'sim' else '' end _1_50, case ic_2_00_pc when 1 then 'sim' else '' end _2_00, case ic_2_50_pc when 1 then 'sim' else '' end _2_50, case ic_3_00_pc when 1 then 'sim' else '' end _3_00, vr_result_1_00_pc as vr_1_00, vr_result_1_50_pc as vr_1_50, vr_result_2_00_pc as vr_2_00, vr_result_2_50_pc as vr_2_50, vr_result_3_00_pc as vr_3_00
FROM hist_dados WHERE cd_acao LIKE 'BIDI11%' and  dt_pregao <> '2019-01-02' ORDER BY 2 DESC , 1 DESC

-- Ações com melhores médias de ganho na abertura dentro das ações que mais bateram 1%
SELECT A.cd_acao, avg(A.pc_abertura), AVG(A.vr_volume)
FROM hist_dados A
INNER JOIN (select cd_acao, ic_1_00_pc from hist_dados
			where dt_pregao in (select distinct dt_pregao 
			from hist_dados order by 1 DESC limit 25)
			group by cd_acao having sum(ic_1_00_pc) >= 22) B
ON A.cd_acao = B.cd_acao
where A.dt_pregao <> '2019-01-02'
group by A.cd_acao having AVG(A.vr_volume) > 1000000
order by 2 desc

"""
vr_investimento_padrao = 20000
sql_insert = 'INSERT INTO hist_dados VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'


def registros(ano1, ano2):
    lista1, lista2 = (), ()

    f = open("COTAHIST_A" + str(ano1) + ".TXT", "r")
    if f.mode == 'r':
        lista1 = f.readlines()

    f = open("COTAHIST_A" + str(ano2) + ".TXT", "r")
    if f.mode == 'r':
        lista2 = f.readlines()

    return lista1 + lista2


def conecta_db(con):
    try:
        cur = con.cursor()
        cur.execute('SELECT SQLITE_VERSION()')
        data = cur.fetchone()[0]

        print("\n=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=")
        print(" SQLite version: {}".format(data))
        print(" Carga de dados históricos Bovespa")
        print(" Processando...")

        sqlcreate = 'CREATE TABLE IF NOT EXISTS hist_dados ' \
                    '(cd_acao VARCHAR(12), ' \
                    'dt_pregao VARCHAR(10), ' \
                    'vr_fechamento FLOAT, ' \
                    'vr_volume INTEGER, ' \
                    'pc_variacao FLOAT, ' \
                    'vr_maximo_dia FLOAT, ' \
                    'vr_minimo_dia FLOAT, ' \
                    'pc_maximo_dia FLOAT, ' \
                    'pc_minimo_dia FLOAT, ' \
                    'vr_abertura FLOAT, ' \
                    'pc_abertura FLOAT, ' \
                    'ic_1_00_pc INTEGER, ' \
                    'ic_1_50_pc INTEGER, ' \
                    'ic_2_00_pc INTEGER, ' \
                    'ic_2_50_pc INTEGER, ' \
                    'ic_3_00_pc INTEGER, ' \
                    'vr_result_1_00_pc FLOAT, ' \
                    'vr_result_1_50_pc FLOAT, ' \
                    'vr_result_2_00_pc FLOAT, ' \
                    'vr_result_2_50_pc FLOAT, ' \
                    'vr_result_3_00_pc FLOAT, ' \
                    'PRIMARY KEY (cd_acao, dt_pregao))'

        cur.execute(sqlcreate)
        return cur

    except lite.Error as e:
        print("Error {}:".format(e.args[0]))
        sys.exit(1)


def formata_ordena_lista(lista):
    lista_aux = []
    for reg in lista:
        lista_aux.append(reg[0:2] + reg[12:24] + reg[2:12] + reg[24:245])
    lista_aux.sort(reverse=True)

    return lista_aux


def registro_acoes(reg, lista_aux, TIPREGprox, x):

    cd_acao = reg[2:14]
    dt_pregao = reg[14:18] + "-" + reg[18:20] + "-" + reg[20:22]
    vr_fechamento = float(reg[108:121]) / 100
    if TIPREGprox == 0:
        vr_fechamento_ant = 1
    else:
        vr_fechamento_ant = float(lista_aux[x + 1][108:121]) / 100
    vr_volume = float(reg[170:188]) / 100
    pc_variacao = round(((vr_fechamento / vr_fechamento_ant) - 1) * 100, 2)
    vr_maximo_dia = float(reg[69:82]) / 100
    vr_minimo_dia = float(reg[82:95]) / 100
    pc_maximo_dia = round(((vr_maximo_dia / vr_fechamento_ant) - 1) * 100, 2)
    pc_minimo_dia = round(((vr_minimo_dia / vr_fechamento_ant) - 1) * 100, 2)
    vr_abertura = float(reg[56:69]) / 100
    pc_abertura = round(((vr_abertura / vr_fechamento_ant) - 1) * 100, 2)
    if pc_maximo_dia > 1:
        ic_1_00_pc = 1
    else:
        ic_1_00_pc = 0

    if pc_maximo_dia > 1.5:
        ic_1_50_pc = 1
    else:
        ic_1_50_pc = 0

    if pc_maximo_dia > 2:
        ic_2_00_pc = 1
    else:
        ic_2_00_pc = 0

    if pc_maximo_dia > 2.5:
        ic_2_50_pc = 1
    else:
        ic_2_50_pc = 0

    if pc_maximo_dia > 3:
        ic_3_00_pc = 1
    else:
        ic_3_00_pc = 0

    if ic_1_00_pc == 1:
        vr_result_1_00_pc = vr_investimento_padrao * 0.01
    else:
        vr_result_1_00_pc = (vr_investimento_padrao * pc_variacao) / 100

    if ic_1_50_pc == 1:
        vr_result_1_50_pc = vr_investimento_padrao * 0.015
    else:
        vr_result_1_50_pc = (vr_investimento_padrao * pc_variacao) / 100

    if ic_2_00_pc == 1:
        vr_result_2_00_pc = vr_investimento_padrao * 0.02
    else:
        vr_result_2_00_pc = (vr_investimento_padrao * pc_variacao) / 100

    if ic_2_50_pc == 1:
        vr_result_2_50_pc = vr_investimento_padrao * 0.025
    else:
        vr_result_2_50_pc = (vr_investimento_padrao * pc_variacao) / 100

    if ic_2_00_pc == 1:
        vr_result_3_00_pc = vr_investimento_padrao * 0.03
    else:
        vr_result_3_00_pc = (vr_investimento_padrao * pc_variacao) / 100

    return [cd_acao, dt_pregao, vr_fechamento, vr_volume, pc_variacao,
            vr_maximo_dia, vr_minimo_dia, pc_maximo_dia, pc_minimo_dia, vr_abertura, pc_abertura,
            ic_1_00_pc, ic_1_50_pc, ic_2_00_pc, ic_2_50_pc, ic_3_00_pc,
            vr_result_1_00_pc, vr_result_1_50_pc, vr_result_2_00_pc, vr_result_2_50_pc, vr_result_3_00_pc]


def main():
    con = lite.connect('acoes.v5.db')
    cur = conecta_db(con)
    lista = registros(2020, 2021)
    lista_aux = formata_ordena_lista(lista)

    x, cont_reg_inseridos, TIPREGprox = 0, 0, 0
    for reg in lista_aux:
        TIPREG = int(reg[0:2])
        if TIPREG == 1:
            TIPREGprox = int(lista_aux[x+1][0:2])  # Para evitar erro
        CODBDI = reg[22:24]
        if TIPREG == 1 and CODBDI == '02':
            cont_reg_inseridos += 1

            try:
                cur.execute(sql_insert, registro_acoes(reg, lista_aux, TIPREGprox, x))

            except lite.Error as e:
                # print("Error {}:".format(e.args[0]))
                # sys.exit(1)
                cont_reg_inseridos -= 1
        x += 1

    if con:
        con.commit()
        con.close()

    if cont_reg_inseridos > 0:
        print("\n Carga concluída com sucesso!")
        print(" " + str(cont_reg_inseridos) + " registros incluídos.")
    else:
        print("\n Não há registros para inclusão.")

    print("=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=")


if __name__ == "__main__":
    main()
