import mysql.connector
from mysql.connector import Error
import pandas as pd
from glob import glob
from funcoes import entrar_mysql, criar_conexao_bd, requisicao, inicializacao, retorna_valores, leitura
from datetime import datetime
import gzip
import os

##############################################################################################################################################################
# conecta banco de dados

maquina, usuario, senha, banco_de_dados = inicializacao()
connection=criar_conexao_bd(maquina, usuario, senha, banco_de_dados)


##############################################################################################################################################################

## jogando dados para dataframe ##
def INMET():
   
    entrada='/p1-xonny2/gerop/testes/observados_DB/gabriela/INMET/*' #dados/BKP_METAR/2*/UND*'
    coletanea = sorted(glob(entrada))
    print ('--------------------------------Vou abrir os dados do INMET--------------------------------\n\n')   
    for arquivos in coletanea:

        # le o arquivo dados
        dados = pd.read_csv(arquivos, compression='gzip', sep=r'\s+', skiprows=1, names=['ESTACAO', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'ANO', 'MES', 'DIA', 'HORA', 'TEMP(C)','TMAX(C)','TMIN(C)','UR(%)','URMAX(%)','URMIN(%)','TD(C)','TDMAX(C)','TDMIN(C)','PRESSAONNM(hPa)','PRESSAONNM_MAX(hPa)','PRESSAONNM_MIN(hPa)','VELVENTO(m/s)','DIR_VENTO','RAJADA(m/s)','RADIACAO(Kjm2)','PRECIPITACAO(mm)'])
          
        # adiciona coluna data
        dados['DATA'] = pd.to_datetime({
            'year': dados['ANO'],   
            'month': dados['MES'],  
            'day': dados['DIA'],
            'hour': dados['HORA']     
        })
        dados['DATA'] = pd.to_datetime(dados['DATA'], format='%Y%m%d %H%M%S')
        dados['DATA'] = dados['DATA'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        colunas = ['ESTACAO', 'DATA'] + [col for col in dados.columns[:] if col not in ['ESTACAO', 'DATA']]
        dados= dados[colunas]
        dados= dados.drop(columns=['ANO'])
        dados=dados.drop(columns=['MES'])
        dados=dados.drop(columns=['DIA'])
        dados=dados.drop(columns=['HORA'])
        
        print ('--------------------------------Vou inserir os dados no BD OBSERVADOS--------------------------------\n\n')
        # adiciona no banco de dados
        for index, row in dados.iterrows():

            valores_estacoes = (row['ESTACAO'], row['LONGITUDE'], row['LATITUDE'], row['ALTITUDE'])
            query_estacoes = "INSERT IGNORE INTO OBSERVADOS.INMET_ESTACOES (`estacao`, `longitude`, `latitude`, `altitude`) VALUES (%s, %s, %s, %s)" #20
            retorna_valores(connection, query_estacoes, valores_estacoes)
            

            valores = (str(row['ESTACAO']), str(row['DATA']), row['TEMP(C)'], row['TMAX(C)'], row['TMIN(C)'], row['UR(%)'], row['URMAX(%)'], row['URMIN(%)'], row['TD(C)'], row['TDMAX(C)'], row['TDMIN(C)'], row['PRESSAONNM(hPa)'], row['PRESSAONNM_MAX(hPa)'], row['PRESSAONNM_MIN(hPa)'], row['VELVENTO(m/s)'],row['DIR_VENTO'],row['RAJADA(m/s)'],row['RADIACAO(Kjm2)'], row['PRECIPITACAO(mm)'])
            query = "INSERT IGNORE INTO OBSERVADOS.INMET (`estacao`, `data`, `temp.ins.(C)`, `temp.max.(C)`, `temp.min.(C)`, `umi.ins.(%)`, `umi.max.(%)`, `umi.min.(%)`, `PtoOrvalhoIns.(C)`, `PtoOrvalhoMax.(C)`, `PtoOrvalhoMin.(C)`, `PressaoIns.(hPa)`, `PressaoMax.(hPa)`, `PressaoMin.(hPa)`, `vel.vento(m/s)`, `dir.vento(m/s)`, `raj.vento(m/s)`, `radiacao(KJ/m²)`, `chuva (mm)`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            retorna_valores(connection, query, valores)
##############################################################################################################################################################
def SYNOP():

     #perguntar ao usuário o caminho do arquivo
    #entrada = input("Por favor, forneça o caminho completo do arquivo que deseja inserir no bd: ")
    entrada='/p1-xonny2/gerop/testes/observados_DB/gabriela/SYNOP/U*'
    coletanea = sorted(glob(entrada))
    print ('--------------------------------Vou abrir os dados do SYNOP--------------------------------\n\n')
    for arquivos in coletanea:
        dados = pd.read_csv(arquivos, skiprows = 1, sep='\\s+', names=['ESTACAO', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'COBERTURA_DE_NUVENS', 'DIR_NUVENS', 'INT_VENTO(m/s)', 'U(m/s)', 'V(m/s)', 'TEMP', 'TD', 'TN', 'TX', 'SLP', 'PCP24H', 'FENOM', 'PRESSAO_ESTACAO'], usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
        
        data = os.path.basename(arquivos)
        data = data[10:22]
        dados['DATA'] = data#datetime.strptime(data, '%Y-%m-%d-%H:%M:%S')
        dados['DATA']=pd.to_datetime(dados['DATA'], format='%Y%m%d %H%M%S')
        dados['DATA'] = dados['DATA'].dt.strftime('%Y-%m-%d %H:%M:%S')

        colunas = ['ESTACAO', 'DATA'] + [col for col in dados.columns[:] if col not in ['ESTACAO', 'DATA']]
        dados= dados[colunas]

     
        print ('--------------------------------Vou inserir os dados no BD OBSERVADOS--------------------------------\n\n')
         # prencher tabela SYNOP
        for index, row in dados.iterrows():

                valores = (row['ESTACAO'], str(row['DATA']), row['COBERTURA_DE_NUVENS'], row['DIR_VENTO'], row['INT_VENTO(m/s)'], row['U(m/s)'], row['V(m/s)'], row['TEMP'], row['TD'], row['TN'], row['TX'], row['SLP'], row['PCP24H'], row['FENOM'], row['PRESSAO_ESTACAO'])
                query = "INSERT INTO OBSERVADOS.SYNOP (`estacao`, `data`, `cobertura_nuvens`, `dir_vento`, `int_vento(m/s)`, `vento_zonal(m/s)`, `vento_meridional(m/s)`, `temperatura`,  `temperatura_orvalho`, `temperatura_minima`, `temperatura_maxima`, `slp`, `pcp24h`, `fenomeno`, `stn_pres`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                retorna_valores(connection, query, valores)

                # prencher tabela SYNOP ESTACOES
                query_estacoes = "INSERT INTO OBSERVADOS.SYNOP_ESTACOES (`estacao`, `latitude`, `longitude`, `altitude`) VALUES (%s, %s, %s, %s)"
                valores_estacoes = (str(row['ESTACAO']), row['LATITUDE'], row['LONGITUDE'], row['ALTITUDE'])
                retorna_valores(connection, query_estacoes, valores_estacoes)

##############################################################################################################################################################
def METAR():

    entrada='/p1-xonny2/gerop/testes/observados_DB/gabriela/METAR/U*' 
    #entrada = input("Por favor, forneça o caminho completo do arquivo que deseja inserir no bd: ")
    coletanea = sorted(glob(entrada))
    
    print ('--------------------------------Vou abrir os dados do METAR--------------------------------\n\n')
    for arquivo in coletanea:
        print (arquivo)
        print ('\n')
        
        dados = pd.read_csv(arquivo, compression='gzip', skiprows = 11, sep='\\s+', names=['ANO', 'MES', 'DIA', 'HORA', 'ESTACAO', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'VEL_VENTO', 'DIR_VENTO', 'TEMP', 'TD', 'SLP', 'UR', 'VISIBILIDADE', 'FENOMENO', 'COBERTURA_NUVENS'], usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13, 14, 15, 16])  # 15

        ### configurando coluna data ### 
        datas = pd.read_csv(arquivo, skiprows = 11, sep='\\s+', names=['ANO', 'MES', 'DIA', 'HORA'], usecols=[0,1,2,3])
#        print (datas['HORA'])
        
        dados['DATA']=pd.to_datetime({'year': datas['ANO'], 'month': datas['MES'], 'day': datas['DIA'], 'hour':datas['HORA']/100})#, 'hour':datas['HORA']})

        dados['DATA']=dados['DATA'].dt.strftime('%Y-%m-%d %H:%M:%S')
        #print (dados['DATA'])
        
        colunas = ['DATA'] + [col for col in dados.columns if col not in ['DATA']]
       # print (dados['DATA'])
        dados= dados[colunas]
        dados= dados.drop(columns=['ANO'])
        dados=dados.drop(columns=['MES'])
        dados=dados.drop(columns=['DIA'])
        dados=dados.drop(columns=['HORA'])
        #print (dados['DATA'])
        print ('--------------------------------Vou inserir os dados no BD OBSERVADOS--------------------------------\n\n')
        # prencher tabela METAR
        for index, row in dados.iterrows():
        	consulta_sql = f"SELECT * FROM OBSERVADOS.METAR WHERE estacao = '{str(row['ESTACAO'])}' AND data = '{row['DATA']}';" 
        	
        	linhas=leitura(connection, consulta_sql)
        	data_existente= datetime.strptime(row['DATA'], '%Y-%m-%d %H:%M:%S')
        	if not linhas or data_existente != linhas[0] and row['ESTACAO'] != linhas[1]:
        		# prencher tabela METAR
        		query = """
                INSERT INTO OBSERVADOS.METAR 
                (`data`, `estacao`, `vel_vento(m/s)`, `dir_vento(deg)`, `temperatura(C)`, `pto_orvalho(C)`, `slp(Pa)`, `UR(%)`, `visibilidade(m)`, `fenomeno`, `cobertura_nuvens`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        		valores = (row['DATA'], str(row['ESTACAO']), row['VEL_VENTO'], row['DIR_VENTO'], row['TEMP'], row['TD'],row['SLP'], row['UR'], row['VISIBILIDADE'], row['FENOMENO'], row['COBERTURA_NUVENS'])
                
        		retorna_valores(connection, query, valores)
                

          # prencher tabela METAR_ESTACOES
        	query = """
            INSERT INTO OBSERVADOS.METAR_ESTACOES 
            (`estacao`, `longitude`, `latitude`)
            VALUES (%s, %s, %s)
            """
        	valores = (row['ESTACAO'], row['LONGITUDE'], row['LATITUDE'])
        	retorna_valores(connection, query, valores)

##############################################################################################################################################################
#INMET() 

#SYNOP()

METAR()

quit()



'''

cursor = connection.cursor() #buffered=True
consulta_sql = f"SELECT * FROM OBSERVADOS.METAR WHERE estacao = '{str(row['ESTACAO'])}' AND data = '{row['DATA']}';"
cursor.execute(consulta_sql)
linhas=cursor.fetchone() #[0:2]

'''

