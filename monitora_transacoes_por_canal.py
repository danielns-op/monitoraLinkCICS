#!/usr/bin/python3
# -*- coding: utf-8 -*-
# - monitora_transacoes_por_canal.py -------------------------------- #
# ------------------------------------------------------------------- #
# Author: Daniel Noronha                                              #
#  Email: danielns.py@gmail.com                                       #
# ------------------------------------------------------------------- #

# - Imports --------------------------------------------------------- #
from consultaDB2 import ConsultaDB2
from datetime import datetime
from dotenv import load_dotenv
from os import getenv, path, mkdir, stat

import csv
import logging
import pandas as pd
import shutil
# ------------------------------------------------------------------- #

# - Carregando variáveis de ambiente -------------------------------- #
load_dotenv()
# ------------------------------------------------------------------- #

# - Variáveis ------------------------------------------------------- #
TABELA_VISA_DIRETO = getenv("TABELA_DIRETO")
NUMERO_LINKS = [1, 2, 3, 4]

DIR_NINSOFT = ""

FILE_DIR_LOCAL = ""
FILE_DATA_1 = f"link_1_dados_transacoes_{datetime.now().day}{datetime.now().month}{datetime.now().year}.csv"
FILE_DATA_2 = f"link_2_dados_transacoes_{datetime.now().day}{datetime.now().month}{datetime.now().year}.csv"
FILE_DATA_3 = f"link_3_dados_transacoes_{datetime.now().day}{datetime.now().month}{datetime.now().year}.csv"
FILE_DATA_4 = f"link_4_dados_transacoes_{datetime.now().day}{datetime.now().month}{datetime.now().year}.csv"

FILE_LIST = {
  "file_1": FILE_DATA_1,
  "file_2": FILE_DATA_2,
  "file_3": FILE_DATA_3,
  "file_4": FILE_DATA_4,
}

DIR_LOG = ''
ARQUIVO_LOG_1 = f"link_1_visa_{datetime.now().day}{datetime.now().month}{datetime.now().year}.log"
ARQUIVO_LOG_2 = f"link_2_visa_{datetime.now().day}{datetime.now().month}{datetime.now().year}.log"
ARQUIVO_LOG_3 = f"link_3_visa_{datetime.now().day}{datetime.now().month}{datetime.now().year}.log"
ARQUIVO_LOG_4 = f"link_4_visa_{datetime.now().day}{datetime.now().month}{datetime.now().year}.log"

LOG_LIST = {
  "log_1": ARQUIVO_LOG_1,
  "log_2": ARQUIVO_LOG_2,
  "log_3": ARQUIVO_LOG_3,
  "log_4": ARQUIVO_LOG_4
}

# ------------------------------------------------------------------- #

# - Funções --------------------------------------------------------- #
def grava_info(logfile, message):
  log = logging.getLogger()
  log.setLevel(logging.INFO)
  file_handle = logging.FileHandler(filename=logfile)
  file_handle.setLevel(logging.INFO)
  formatter = logging.Formatter(
    fmt="%(levelname)s %(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
  )
  file_handle.setFormatter(formatter)
  log.addHandler(file_handle)
  log.info(message)
  log.removeHandler(file_handle)
  del log, file_handle


def grava_erro(logfile, message):
  log = logging.getLogger()
  log.setLevel(logging.ERROR)
  file_handle = logging.FileHandler(filename=logfile)
  file_handle.setLevel(logging.ERROR)
  formatter = logging.Formatter(
    fmt="%(levelname)s %(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
  )
  file_handle.setFormatter(formatter)
  log.addHandler(file_handle)
  log.error(message)
  log.removeHandler(file_handle)
  del log, file_handle


def verifica_arquivos_locais():
  """
    Verificar se os arquivos onde será gravado as transações e o log existe,
    se não os arquivos serão criados.
  """
  if not path.isdir(FILE_DIR_LOCAL):
    mkdir(FILE_DIR_LOCAL)

  if not path.isdir(DIR_LOG):
    mkdir(DIR_LOG)

  for link in NUMERO_LINKS:
    file_arquivo = f"{FILE_DIR_LOCAL}/{FILE_LIST[f'file_{link}']}"
    log_arquivo = f"{DIR_LOG}/{LOG_LIST[f'log_{link}']}"

    if not path.isfile(f"{file_arquivo}"):
      file = open(f"{file_arquivo}", "a")
      file.close()

    if not path.isfile(f"{log_arquivo}"):
      file = open(f"{log_arquivo}", "a")
      file.close()


def consulta_transacoes(link):
  """
    Consulta o total de transações do visa por link.

  Args:
      link (int): Link no qual deverá ser realizado a consulta.

  Returns:
      list: Duas lista, uma contendo o nome das colunas e a outra
            contendo a última transação do dia.
  """
  consulta = ConsultaDB2(tabela=TABELA_VISA_DIRETO)
  select = consulta.retorna_todas_transacoes_visa_direto_por_link(link)
  dados_object = consulta.realizar_cosulta(select)

  coluna = dados_object.columns
  valores = dados_object.values[-1]

  return coluna, valores


def trata_dados(*args):
  # A coluna vem com um total de 10 elementos, posição 0 á 9 conforme abaixo:
  # ['CANAL', 'DATA ATUAL', 'HORA ATUAL', 'ID', 'DATA DO MOVIMENTO', 'DATA DAS TRANSAÇÕES', 'HORA DAS TRANSAÇÕES', 'QUANTIDADE DE COMPRAS', 'QUANTIDADE DE OCORRÊNCIAS', 'LINK']
  colunas = args[0]

  # Os valores vem em lista de 10 elementos, posição de 0 á 9 conforme abaixo:
  # ['VISA-DIRETO', datetime.date(2022, 12, 7), '15.40.21', 751819.0, datetime.date(2022, 12, 7), 1207.0, 154016.0, 1, 0, 1.0]
  valores = args[1]

  # Iremos apenas utilizar 4 elementos.
  # coluna: ['LINK', 'ID', 'DATA DO MOVIMENTO', 'HORA DAS TRANSAÇÕES']
  # valores: [1, 752714, datetime.date(2022, 12, 7), 154918.0]
  nova_coluna = [colunas[9], colunas[3], colunas[4], colunas[6]]
  novos_valores = [int(valores[9]), int(valores[3]), valores[4], valores[6]]

  # Para as transaçõs do VISA quando a hora for meia noite os zeros são omitidos.
  # Ex.: 4, significa 00:00:04 ou seja, 0 Horas 0 minutos e 4 segundos.
  # Com isso em mente, precisamos testar o tamanho da string e se for menor que 6
  # completar os tamanhos com 0.
  hora_string = str(int(novos_valores[-1]))

  while len(hora_string) < 6:
    hora_string = "0" + hora_string

  # O horário das transações vem no formato de número, Ex.: 80912.0 -> 08:09:12
  # Então pegamos o valor da chave 'HORA DAS TRANSACOES' e transformamos em
  # Inteiro int(dados_linha['HORA DAS TRANSACOES']) para retirar o ponto flutuante .0,
  # logo após transformamos em String str(int(dados_linha['HORA DAS TRANSACOES']))
  # para que possamos utilizar o fatiamento de string, dessa forma podemos pegar cada
  # valor conforme abaixo:
  # Usando o horário como 91013
  #   Hora => str(int(dados_linha['HORA DAS TRANSACOES']))[:-4] -> vai pegar
  #     qualquer valor do 0 ate o segundo elemento, com isso teremos o valor 9, de 9 horas.
  #   Minuto => str(int(dados_linha['HORA DAS TRANSACOES']))[-4:-2] -> vai pegar
  #     o elemento -4 ate o -3, lembrando que o -2 não entra na contagem, com isso teremos
  #     o valor 10, de 10 minutos.
  #   Segundos => str(int(dados_linha['HORA DAS TRANSACOES']))[-2:] -> vai pegar da posição
  #     -2 ate o final da string, com isso teremos o valor 13, de 13 segundos.
  # Para ajudar a visualização:
  #     Número ->   9  1  0  1  3
  #     Posição -> -5 -4 -3 -2 -1

  hora = f"{hora_string[:-4]}:{hora_string[-4:-2]}:{hora_string[-2:]}"

  novos_valores[-1] = hora

  return nova_coluna, novos_valores


def verifica_dados_duplicado(linha, arquivo):
  """
    Verifica se a linha que será gravada é a mesma transação que se
    já encontra no arquivo.

  Args:
      linha (list): Uma lista contendo os dados da última transação que será salvo.
      arquivo (string): Caminho absoluto do arquivo onde os dados estão sendo salvos.

  Returns:
      boolean: Caso a transação já exista no arquivo retorna 'True' se não, retorna 'False'.
  """
  ultima_linha = pd.read_csv(arquivo).values[-1]
  if linha[1] == ultima_linha[1]:
    return True
  else:
    return False


def checa_tempo_transacao(*args):
  data_hora_atual = datetime.strptime(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

  canal = args[0]
  data_transacao = args[1]
  hora_transacao = args[2]
  data_hora_transacao = datetime.strptime(f"{data_transacao} {hora_transacao}", "%Y-%m-%d %H:%M:%S")

  diferenca_tempo = data_hora_atual - data_hora_transacao

  # Definindo arquivo do log.
  log_file = f"{DIR_LOG}/{LOG_LIST[f'log_{canal}']}"

  if diferenca_tempo.total_seconds() >= 900.0:
    message = f"ERROR - LINK {canal} - Última transação foi realizada á {diferenca_tempo} atrás."
    grava_erro(logfile=log_file, message=message)
  else:
    message = f"SUCESSO - LINK {canal} - Última transação foi realizada á {diferenca_tempo} atrás."
    grava_info(logfile=log_file, message=message)


def grava_no_csv(coluna, linha, link):
  """
    Grava as informações das transações do VISA em um arquivo .csv.
    Esse arquivo será utilizada para checar se o LINK está com 15 minutos
    ou mais sem processar transações.

  Args:
      coluna (list): Lista contendo o nome das colunas que serão salvas no
                     arquivo csv.
          Exemplo: ["LINK", "ID", "DATA DO MOVIMENTO", "HORA DAS TRANSAÇÕES"]

      linha (list): Lista contendo alguns dados das transações.
          Exemplo: [1, 800626, "2022-12-08", "08:47:05"]

      link (int): Número do link. Essa informação é importante poís, esse número
                  será utilizado para direcionar os dados para o arquivo correto.

  Returns:
      Retorna duas variáveis.
      status (boolean): Se 'False' significa que a transação não existe no arquivo
                        e a mesma foi gravada com sucesso. Se 'True' significa que
                        a transação já existe e com isso deve ser tomado o próximo
                        passo que é verificar quando tempo está sem passar transações.
      transacao (None or list): Caso a transação já exista no arquivo essa variável ira
                              receber uma lista contendo a transação repetida para que
                              possa ser verificado quanto tempo está sem passaar transações.
                              O padrão dessa variável é 'None'.
  """
  status = False
  transacao = None

  arquivo = f"{FILE_DIR_LOCAL}/{FILE_LIST[f'file_{link}']}"

  # verificar se já há dados no arquivo.
  esta_vazio = stat(f"{arquivo}").st_size == 0

  with open(f"{arquivo}", 'a', newline='', encoding='utf-8') as file:
    escrita = csv.writer(file)

    # se retornar 'True' significa que o arquivo é novo e dever ser gravado
    # a primeira linha com o nome das colunas.
    if esta_vazio:
      escrita.writerow(coluna)
      escrita.writerow(linha)
    else:
      if verifica_dados_duplicado(linha, arquivo):
        status = True
      else:
        escrita.writerow(linha)
  file.close()

  transacao = linha

  return status, transacao


def copia_para_fileserver(filename):
  dir_origem = ""
  dir_destino = DIR_NINSOFT

  if path.exists(f"{dir_destino}"):
    try:
      shutil.copyfile(f"{dir_origem}{filename}", f"{dir_destino}/{filename}")
    except Exception as error:
      print(f"ERROR - Não foi possível copiar o arquivo '{filename}' para o diretório {dir_destino}.\n{error}")
  else:
    print(f"ERROR - O Diretório {dir_destino}logs não está disponível.\nValidar se o compatilhamento está ativo.")


def main():
  verifica_arquivos_locais()

  for link in NUMERO_LINKS:
    try:
      coluna, dados = consulta_transacoes(link)
      coluna, dados = trata_dados(coluna, dados)

      transacao_existe, transacao = grava_no_csv(coluna, dados, link)

      canal = transacao[0]
      data = transacao[2]
      hora = transacao[3]

      checa_tempo_transacao(canal, data, hora)
      log_file = f"{LOG_LIST[f'log_{canal}']}"
      copia_para_fileserver(log_file)

    except Exception as error:
      print(f"INFO - MONITORAÇÃO VISA - Não há transações para o LINK '{link}'.")


# ------------------------------------------------------------------- #

# - Execução -------------------------------------------------------- #
if __name__ == "__main__":
  main()
# ------------------------------------------------------------------- #
