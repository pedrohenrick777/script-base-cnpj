from io import TextIOBase


class StreamerTextoIO(TextIOBase):
    """
    Classe 'file-like' para ser lida pelo COPY do DBAPI que se utiliza de um gerador, permitindo formatar o arquivo
    sem ele ser totalmente carregado na RAM
    """

    def __init__(self, iterador):
        self._iter = iterador
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n=None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n=None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)
