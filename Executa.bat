@echo off
ECHO.
ECHO Extraindo dados hist¢ricos.
rem winrar x -o arquivos\COTAHIST_A2021.ZIP
7z e -y arquivos\COTAHIST_A2021.ZIP -oarquivos\
ECHO Conclu¡do!

python "Carregando Dados B3 em banco de dados.py"
ECHO.
set /p resp=Excluir arquivo zip? (S/N) 
IF '%resp%'=='s' GOTO excluir
IF '%resp%'=='S' GOTO excluir
IF NOT '%resp%'=='s' GOTO manter

:excluir
del arquivos\COTAHIST_A2021.ZIP
ECHO Arquivo exclu¡do!
ECHO.
GOTO fim

:manter
ECHO Arquivo mantido!
ECHO.

:fim
del arquivos\COTAHIST_A2021.TXT
start acoes.v5.db /autostart