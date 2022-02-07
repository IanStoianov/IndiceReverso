#################################################################
#       Criação de índice reverso                               #
#       Autor: Ian Stoianov Loureiro                            #
#       Data: 07/02/2022                                        #                    
#                                                               #
#       Passar no primeiro parâmetro o path do dataset          #
#                                                               #
#################################################################
import sys
import re
import os
import pandas as pd

#python 'C:\Users\ianst\Downloads\IndiceReverso.py' 'C:\Users\ianst\Downloads\dataset'

#Recebe parametro do path do dataset
path=sys.argv[1]

def indiceReverso(path):
    #Inicializa listas
    emp = []
    palavras=[]
    indice=[]

    #Carrega arquivos, limpa caracteres indesejados, e identifica todas as palavras unicas por documento e associa ao nome do documento em uma lista de tuplas, além de criar lista de todas as palavras
    for filename in os.listdir(path):
        with open(os.path.join(path, filename), 'r') as f:
            arquivo = re.sub('\'(s\w)', "\\1", re.sub('^-', "", re.sub('\'(?!s)|^\'s$', "", f.read()))).replace('\n', ' ').replace('(', ' ').replace(')', ' ').replace('[', ' ').replace(']', ' ').replace('*', ' ').replace(',', ' ').replace('.', ' ').replace(':', ' ').replace(';', ' ').replace('?', ' ').replace('!', ' ').replace('#', ' ').replace('|', ' ').replace('_', ' ').replace('{', ' ').replace('}', ' ').replace('~', ' ').replace('--', ' ').replace('\t', ' ').replace('/', ' ').replace('‡', ' ').replace('†', ' ').replace('\"', ' ').replace('$', ' ').replace('&', ' ').replace('+', ' ').replace('=', ' ').replace('<', ' ').replace('>', ' ').split(' ')
            #arquivo = re.sub('\'(?!s)|\'(s\w)', "\\1",  f.read()).replace('\n', ' ').replace('(', ' ').replace(')', ' ').replace('[', ' ').replace(']', ' ').replace('*', ' ').replace(',', ' ').replace('.', ' ').replace(':', ' ').replace(';', ' ').replace('?', ' ').replace('!', ' ').replace('#', ' ').replace('|', ' ').replace('_', ' ').replace('{', ' ').replace('}', ' ').replace('~', ' ').replace('--', ' ').replace('\t', ' ').replace('/', ' ').replace('‡', ' ').replace('†', ' ').replace('\"', ' ').replace('$', ' ').replace('&', ' ').replace('+', ' ').replace('=', ' ').replace('<', ' ').replace('>', ' ').split(' ')
            unico=list(set(arquivo))
            palavras=palavras+unico
            document_id=[filename]*len(unico)
            emp=emp+list(zip(document_id, unico))
   
    #Cria índice das palavras
    unico=sorted(set(palavras))[1:]
    indice=list(zip(list(range(len(unico))), unico))

    #Cria DataFrames a partir dos índices criados acima
    columnsIndice=['word_id', 'palavra']
    dfIndice = pd.DataFrame(indice, columns=columnsIndice)

    columnsEmp = ['document_id', 'palavra']
    dfEmp = pd.DataFrame(emp, columns=columnsEmp)

    #Cria tabela vinculando o word_id com document_id, e converte campo word_id para string
    complete=pd.merge(dfIndice, dfEmp, how="inner", on="palavra")[["document_id", "word_id"]].sort_values(by=['word_id', 'document_id']).astype({"document_id": str})

    #Agrupa document_id para cara word_id para criação do índice reverso
    reverso = complete.groupby(['word_id'])['document_id'].apply(','.join).reset_index()

    return reverso


print(indiceReverso(path))
