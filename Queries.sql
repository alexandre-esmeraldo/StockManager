-- Ações que mais bateram 1% nos últimos x dias com volume > x * 1.000.000
SELECT * FROM (select TRIM(h.cd_acao, " ") as ACAO, sum(ic_1_00_pc) as _1_00pc, sum(ic_1_50_pc) as _1_50pc, sum(ic_2_00_pc) as _2_00pc, sum(ic_2_50_pc) as _2_50pc, sum(ic_3_00_pc) as _3_00pc, round(avg(vr_volume),2) as VOLUME, sum(vr_result_1_00_pc) as sum_100_pc, sum(vr_result_1_50_pc) as sum_150_pc, sum(vr_result_2_00_pc) as sum_200_pc, avg(pc_abertura), avg(vr_fechamento) as avg_vr_fech, url_advfn from hist_dados h left join aluguel_acoes_advfn a on h.cd_acao = a.cd_acao
WHERE dt_pregao in (SELECT DISTINCT dt_pregao 
       FROM hist_dados ORDER BY 1 DESC limit 15)
GROUP BY h.cd_acao HAVING sum(ic_1_00_pc) >= 13) WHERE VOLUME > 6000000 AND avg_vr_fech > 5 ORDER BY 2 DESC, 3 DESC, 4 DESC, 5 DESC, 6 DESC, 7 DESC

-- Detalhes de uma ação específica
SELECT cd_acao, dt_pregao, vr_fechamento as cotacao, vr_volume, pc_variacao as '%', vr_maximo_dia as vr_max, vr_minimo_dia as vr_min, pc_maximo_dia as pc_max, pc_minimo_dia as pc_min, ">>>>" as ">", pc_abertura as abert, "<<<<" as "<", case ic_1_00_pc when 1 then 'sim' else '' end _1_00, case ic_1_50_pc when 1 then 'sim' else '' end _1_50, case ic_2_00_pc when 1 then 'sim' else '' end _2_00, case ic_2_50_pc when 1 then 'sim' else '' end _2_50, case ic_3_00_pc when 1 then 'sim' else '' end _3_00, vr_result_1_00_pc as vr_1_00, vr_result_1_50_pc as vr_1_50, vr_result_2_00_pc as vr_2_00, vr_result_2_50_pc as vr_2_50, vr_result_3_00_pc as vr_3_00
FROM hist_dados WHERE cd_acao LIKE 'MRFG3%' and  dt_pregao <> '2019-01-02' ORDER BY 2 DESC , 1 DESC

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

-- Ações que mais bateram 1% nos últimos x dias
select TRIM(cd_acao, " "), sum(ic_1_00_pc), sum(ic_1_50_pc), sum(ic_2_00_pc), sum(vr_volume), sum(vr_result_1_00_pc), sum(vr_result_1_50_pc), sum(vr_result_2_00_pc) from hist_dados
where dt_pregao in (select distinct dt_pregao 
     from hist_dados order by 1 DESC limit 14)
group by cd_acao having sum(ic_1_00_pc) >= 12 order by 5 DESC

select * FROM hist_dados
WHERE pc_variacao = 9999