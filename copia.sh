#!/bin/bash



# caminho arquivo original #

ARQUIVO_FONTE=("gerop@xonny2:/p1-xonny2/gerop/testes/observados_DB/gabriela/insercao2.py" "gerop@xonny2:/p1-xonny2/gerop/testes/observados_DB/gabriela/cria_banco_de_dados.py")


# CAMINHO DESTINO  #
DESTINO="/home/gabriela/treinamento/python/scripts/OBSERVADOS_DB/"

for arquivo in "${ARQUIVO_FONTE[@]}";do
	echo "Copiando "$arquivo" para "$DESTINO""
	scp "$arquivo" "$DESTINO"
done
