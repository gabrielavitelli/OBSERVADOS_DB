#python
import mysql.connector
from mysql.connector import Error
import pandas as pd
from funcoes import entrar_mysql, criar_conexao_bd, requisicao, inicializacao

###########################################################################################3

def inmet(connection): 
        create_inmet_table="""
                        CREATE TABLE IF NOT EXISTS INMET (
                                `estacao` VARCHAR(40) NOT NULL,
                                `data` DATETIME PRIMARY KEY,
                                `temp.ins.(C)` FLOAT NOT NULL,
                                `temp.max.(C)` FLOAT NOT NULL,
                                `temp.min.(C)` FLOAT NOT NULL,
                                `umi.ins.(%)` FLOAT NOT NULL,
                                `umi.max.(%)` FLOAT NOT NULL,
                                `umi.min.(%)` FLOAT NOT NULL,
                                `PtoOrvalhoIns.(C)` FLOAT NOT NULL,
                                `PtoOrvalhoMax.(C)` FLOAT NOT NULL,
                                `PtoOrvalhoMin.(C)` FLOAT NOT NULL,
                                `PressaoIns.(hPa)` FLOAT NOT NULL,
                                `PressaoMax.(hPa)` FLOAT NOT NULL,
                                `PressaoMin.(hPa)` FLOAT NOT NULL,
                                `vel.vento(m/s)` FLOAT NOT NULL,
                                `dir.vento(m/s)` FLOAT NOT NULL,
                                `raj.vento(m/s)` FLOAT NOT NULL,
                                `radiacao(KJ/m²)` FLOAT NOT NULL,
                                `chuva (mm)` FLOAT NOT NULL)"""
                        
        requisicao(connection, create_inmet_table)
        anexador = "CREATE INDEX idx_estacao ON INMET(estacao);"
        requisicao(connection, anexador)
        print('Tabela INMET criada')
        connection = criar_conexao_bd(maquina, usuario, senha, banco_de_dados)

        create_inmet_estacoes="""
                CREATE TABLE IF NOT EXISTS INMET_ESTACOES (
                        `estacao` VARCHAR(40) PRIMARY KEY,
                        `longitude` FLOAT NOT NULL,
                        `latitude` FLOAT NOT NULL,
                        `altitude` FLOAT NOT NULL)"""
        requisicao(connection, create_inmet_estacoes)
        print('Tabela INMET_ESTACOES criada')



###########################################################################################3
def synop(connection):    
        create_synop_table ="""
                        CREATE TABLE IF NOT EXISTS SYNOP (
                        `estacao` VARCHAR(40) PRIMARY KEY,
                        `data` DATETIME NOT NULL,
                        `cobertura_nuvens` FLOAT NOT NULL,
                        `dir_vento` FLOAT NOT NULL,
                        `int_vento(m/s)` FLOAT NOT NULL,
                        `vento_zonal(m/s)` FLOAT NOT NULL,
                        `vento_meridional(m/s)` FLOAT NOT NULL,
                        `temperatura` FLOAT NOT NULL,
                        `temperatura_orvalho` FLOAT NOT NULL,
                        `temperatura_minima` FLOAT NOT NULL,
                        `temperatura_maxima` FLOAT NOT NULL,
                        `slp` FLOAT NOT NULL,
                        `pcp24h` FLOAT NOT NULL,
                        `fenomeno` FLOAT NOT NULL,
                        `stn_pres` FLOAT NOT NULL )"""
        requisicao(connection, create_synop_table)
        anexador = "CREATE INDEX idx_estacao ON SYNOP(estacao);"
        requisicao(connection, anexador)
        print('Tabela SYNOP criada')

        connection = criar_conexao_bd(maquina, usuario, senha, banco_de_dados)
        create_synop_estacoes ="""
                CREATE TABLE IF NOT EXISTS SYNOP_ESTACOES (
                `estacao` VARCHAR(40) PRIMARY KEY,
                `latitude` FLOAT NOT NULL,
                `longitude` FLOAT NOT NULL,
                `altitude` FLOAT NOT NULL)"""
        requisicao(connection, create_synop_estacoes)
        print('Tabela SYNOP_ESTACOES criada')

###########################################################################################3#`estacao` VARCHAR(40) PRIMARY KEY,
def metar(connection):
        create_metar_table ="""
                CREATE TABLE IF NOT EXISTS METAR (
                `data` DATETIME NOT NULL,
                `estacao` VARCHAR(40) NOT NULL, 
                `vel_vento(m/s)` FLOAT NOT NULL,
                `dir_vento(deg)` FLOAT NOT NULL,
                `temperatura(C)` FLOAT NOT NULL,
                `pto_orvalho(C)` FLOAT NOT NULL,
                `slp(Pa)` FLOAT NOT NULL,
                `UR(%)` FLOAT NOT NULL,
                `visibilidade(m)` FLOAT NOT NULL,
                `fenomeno` FLOAT NOT NULL,
                `cobertura_nuvens` FLOAT NOT NULL)"""
        requisicao(connection, create_metar_table)
        anexador = "CREATE INDEX idx_estacao ON METAR(estacao);"
        requisicao(connection, anexador)
        print('Tabela METAR criada')

        connection = criar_conexao_bd(maquina, usuario, senha, banco_de_dados)
        create_metar_estacoes="""
                CREATE TABLE IF NOT EXISTS METAR_ESTACOES (
                `estacao` VARCHAR(40) PRIMARY KEY,
                `longitude` FLOAT NOT NULL,
                `latitude` FLOAT NOT NULL)"""
        requisicao(connection, create_metar_estacoes)
        print('Tabela METAR_ESTACOES criada')

###########################################################################################3
print('#################################################')
print('Script de criação do banco de dados OBSERVAÇÃO_DB')
print('#################################################')
print('')

###########################################################################################3
### funcao para criar banco de dados ###
maquina, usuario, senha, banco_de_dados = inicializacao()
connection=entrar_mysql(maquina, usuario, senha)
query=f'CREATE DATABASE IF NOT EXISTS {banco_de_dados}'
print ('Verificando existência do banco de dados')
#try:
requisicao(connection, query)
#except:
#	print ('here')
#	query=f'CREATE DATABASE{banco_de_dados}'
#	requisicao(connection, query)
	

###########################################################################################3

escolha = input ( "[1] INMET \n[2] METAR\n[3] SYNOP\n[4] TODOS\n")

connection = criar_conexao_bd(maquina, usuario, senha, banco_de_dados)

###########################################################################################3


###########################################################################################3
### criar colunas no db ###
if escolha == '1':
        print ("vou criar as colunas do INMET")
        inmet(connection)
        

elif escolha == '2':
        print ("vou criar as colunas do METAR")
        metar(connection)
elif escolha == '3':
        print ("vou criar as colunas do SYNOP")
        synop(connection)
elif escolha == '4':
        print ("vou criar as colunas do SYNOP, METAR, INMET")
        inmet(connection)
        metar(connection)
        synop(connection)
connection.close()

###########################################################################################3
