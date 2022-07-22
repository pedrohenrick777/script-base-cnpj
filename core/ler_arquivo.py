import os
from configparser import ConfigParser
from subprocess import run
from zipfile import ZipFile

from database.database import Database
from dotenv import load_dotenv

from core.streamer import StreamerTextoIO

ENCODING = 'ISO-8859-1'
CFG = ConfigParser()
CFG.read('config.ini', 'utf-8')


def config_write(section, option, val):
    CFG.set(section, option, val)
    with open('config.ini', 'w') as config_file:
        CFG.write(config_file)


def linha_para_colunas(col):
    dentro_do_valor, valor, colunas = False, '', []
    for i, char in enumerate(col):
        try:
            if char == '"':
                if dentro_do_valor:
                    try:
                        if col[i+1] == '#' or col[i+1] == '\n':
                            dentro_do_valor = not dentro_do_valor
                        else:
                            valor += char
                    except IndexError:
                        dentro_do_valor = not dentro_do_valor
                        raise
                elif i == 0 or col[i-1] == '#':
                    dentro_do_valor = not dentro_do_valor
                else:
                    valor += char
            else:
                if not dentro_do_valor:
                    if char == '#':
                        raise IndexError
                    else:
                        if char == '\n':
                            continue
                        valor += char
                        if col[i+1] == '\n':
                            raise IndexError
                else:
                    valor += char
        except IndexError:
            colunas.append(valor)
            valor = ''
    return colunas


def _tratar_socios(col, dt):
    col_tratada = []
    cols_usadas = [3, 4, 5, 6, 7, 9, 10, 12, 13, 14]
    for i, val in enumerate(linha_para_colunas(col)):
        if i in cols_usadas:
            col_tratada.append(val)
    col_tratada[0] = col_tratada[0][0:8]
    col_tratada[3] = '' if col_tratada[3] == '***000000**' or col_tratada[3] == '99999999999999' else col_tratada[3]
    col_tratada[7] = '' if col_tratada[7] == '***000000**' else col_tratada[7]
    col_tratada[8] = '' if col_tratada[8] == 'CPF INVALIDO' else col_tratada[8]
    col_tratada[9] = col_tratada[9] if not col_tratada[9] == '00' else ''
    col_tratada.append('')

    return ';'.join(f'"{col}"' for col in [str(dt), *col_tratada]) + '\n'


def _tratar_empresa(col, dt):
    col_tratada = []
    cols_usadas = [3, 5, 13, 29, 30, 31]
    for i, val in enumerate(linha_para_colunas(col)):
        if i == 4:
            if int(val) > 1:
                return ''
        if i == 13:
            if val == '0000':
                return ''
        if i in cols_usadas:
            col_tratada.append(val)
    col_tratada[0] = col_tratada[0][0:8]
    col_tratada[4] = col_tratada[4].replace('.', '')
    col_tratada[4] = '' if col_tratada[4] == 'NA' else col_tratada[4]
    col_tratada.append('')

    return ';'.join(f'"{col}"' for col in [str(dt), *col_tratada]) + '\n'


def _tratar_estabelecimento(col, dt):
    col_tratada = []
    cols_usadas = [3, 6, 7, 8, 9, 10, 11, 14, 15,
                   16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 36, 37]
    for i, val in enumerate(linha_para_colunas(col)):
        if i in cols_usadas:
            col_tratada.append(val)
    col_tratada.insert(1, col_tratada[0][-2:])
    col_tratada.insert(1, col_tratada[0][8:-2])
    col_tratada[0] = col_tratada[0][0:8]
    col_tratada.insert(
        3,
        'true'
        if col_tratada[1] == '0001'
        else 'false')
    col_tratada[6] = '' if col_tratada[6] == 'NA' else col_tratada[6]
    col_tratada.insert(12, '')
    for idx in [21, 23, 25]:
        if len(col_tratada[idx]) > 11 and len(col_tratada[idx]) <= 12:
            val_ddd = col_tratada[idx][0:3].rstrip()
            val_col = col_tratada[idx][4:]
        else:
            val_ddd, val_col = '', ''
        col_tratada.insert(idx, val_ddd)
        col_tratada[idx+1] = val_col
    col_tratada[-1] = '' if col_tratada[-1] == 'NA' or \
        col_tratada[-1] == 'N' else col_tratada[-1]
    col_tratada[-2] = '' if col_tratada[-2] == 'NA' else col_tratada[-2]
    col_tratada.insert(0, str(dt))

    return ';'.join(f'"{col}"' for col in [str(dt), *col_tratada]) + '\n'


def criar_gerador(arquivo_aberto):
    return (a for a in arquivo_aberto)


def estruturar_arquivo(arquivo, dt, tipo_arquivo, tratar, remover_duplicatas=True):
    caminho_arquivo = os.path.join(
        'temp', f'{tipo_arquivo}-{dt}-estruturado.csv')
    if os.path.exists(caminho_arquivo):
        os.remove(caminho_arquivo)
    print(f'estruturando {tipo_arquivo}-{dt}')
    with open(caminho_arquivo, 'a', encoding=ENCODING) as file_temp:
        for i, line in enumerate(arquivo):
            if i > 0:
                file_temp.write(tratar(line.decode(ENCODING), dt))
    if remover_duplicatas:
        print('removendo duplicatas')
        run(
            f'sort {caminho_arquivo} | uniq > {caminho_arquivo + ".temp"} &&'
            + f'mv {caminho_arquivo + ".temp"} {caminho_arquivo}',
            shell=True, check=True
        )

    return caminho_arquivo


def tratar_socios(dt, zip_file):
    csv_filename = 'cnpj_dados_socios_pj.csv'
    with zip_file.open(csv_filename, 'r') as csv_file:
        arquivo_estruturado = estruturar_arquivo(
            csv_file, dt, 'socios', _tratar_socios)
    with open(arquivo_estruturado, 'r', encoding=ENCODING) as csv_file:
        colunas = (
            '(data_base,numero_base,cod_natureza_juridica,nome,documento,cod_qualificacao,data_entrada,cod_pais,' +
            'documento_representante,nome_representante,cod_qualificacao_representante,cod_faixa_etaria)'
        )
        gerador = criar_gerador(csv_file)
        inserir_no_banco(gerador, colunas, 'cnpj_socios')
    os.remove(arquivo_estruturado)


def tratar_empresa(dt, zip_file):
    csv_filename = 'cnpj_dados_cadastrais_pj.csv'
    with zip_file.open(csv_filename, 'r') as csv_file:
        arquivo_estruturado = estruturar_arquivo(
            csv_file, dt, 'empresas', _tratar_empresa
        )
    with open(arquivo_estruturado, 'r', encoding=ENCODING) as csv_file:
        colunas = (
            '(data_base, numero_base, razao_social, cod_natureza_juridica,' +
            'qualificacao_responsavel, capital_social, porte_empresa,' +
            'ente_federativo_responsavel)'
        )
        gerador = criar_gerador(csv_file)
        inserir_no_banco(gerador, colunas, 'cnpj_empresas')
    os.remove(arquivo_estruturado)


def tratar_estabelecimento(dt, zip_file):
    csv_filename = 'cnpj_dados_cadastrais_pj.csv'
    with zip_file.open(csv_filename, 'r') as csv_file:
        arquivo_estruturado = estruturar_arquivo(
            csv_file, dt, 'estabelecimentos', _tratar_estabelecimento, False
        )
    with open(arquivo_estruturado, 'r', encoding=ENCODING) as csv_file:
        colunas = (
            '(data_base, numero_base, numero_ordem, numero_dv, matriz, nome_fantasia, ' +
            'cod_situacao_cadastral, data_situacao_cadastral, cod_motivo_situacao_cadastral, nome_cidade_exterior, ' +
            'cod_pais, data_inicio_atividade, cod_cnae_principal, cods_cnae_secundarios, tipo_logradouro, ' +
            'logradouro, numero, complemento, bairro, cep, uf, cod_municipio, ddd1, telefone1, ddd2, ' +
            'telefone2, ddd_fax, fax, email, situacao_especial, data_situacao_especial)'
        )
        gerador = criar_gerador(csv_file)
        inserir_no_banco(gerador, colunas, 'cnpj_estabelecimentos')
    os.remove(arquivo_estruturado)


def gerar_dbapi():
    load_dotenv()
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    DB_HOST = os.environ.get('DB_HOST')

    return Database(DB_USER, DB_PASS, DB_NAME, host=DB_HOST)


def inserir_no_banco(gerador, colunas, tablename):
    dbapi = gerar_dbapi()
    streamer = StreamerTextoIO(gerador)
    print(f'inserindo dados em {tablename}')
    dbapi.copy_csv_from_stdin(tablename, colunas, streamer)


def executar(dt, arquivo_nome):
    zip_base = os.path.join('bases', arquivo_nome)
    socios_est = bool(CFG.get('concluidas', 'socios'))
    estabelecimento_est = bool(CFG.get('concluidas', 'estabelecimentos'))
    empresas_est = bool(CFG.get('concluidas', 'empresas'))
    with ZipFile(zip_base, 'r') as zip_file:
        if not socios_est:
            tratar_socios(dt, zip_file)
            config_write('concluidas', 'socios', 'true')
        if not estabelecimento_est:
            tratar_empresa(dt, zip_file)
            config_write('concluidas', 'empresas', 'true')
        if not empresas_est:
            tratar_estabelecimento(dt, zip_file)
            config_write('concluidas', 'estabelecimentos', 'true')

    config_write('concluidas', 'estabelecimentos', '')
    config_write('concluidas', 'empresas', '')
    config_write('concluidas', 'socios', '')
