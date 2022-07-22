from datetime import date
from os import remove

from core.ler_arquivo import executar
from wget_download import runcmd

if __name__ == '__main__':
    with open('bases_download.txt', 'r') as bases_download:
        for a in bases_download:
            link, arquivo_nome = a.strip('\n').split(';')
            print(f'Baixando {arquivo_nome}')
            runcmd(f'wget -O bases/{arquivo_nome} {link}', verbose=True)
            date_str = arquivo_nome.split('_')[-1].replace('.zip', '')
            date_format = date(
                int(f'20{date_str[-2:]}'), int(date_str[3:5]), int(date_str[0:2]))
            executar(date_format, arquivo_nome)
            remove(f'bases/{arquivo_nome}')
