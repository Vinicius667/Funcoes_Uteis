import pandas as pd 
import os 
import numpy as np
import sys
import glob
from IPython.display import display


# Variáveis
_PADRAO_CHECAR_VALIDADE = False
_PADRAO_ON_ERROR = "pass"


# Valida CPF. Assume-se que o CPF já está formatado corretamente.
# Caso não esteja formate antes, usando a função formatar_cpf().
# Retorna série booleana. True => válido, False = inválido
def validar_cpf(s: pd.Series):
    s = s.astype(str)
    digitos = s.str.extract(r'(\d)(\d)(\d)\.(\d)(\d)(\d)\.(\d)(\d)(\d)-').astype(np.uint8,errors="ignore")
    all_equal = digitos.sub(digitos[0],axis=0).sum(axis=1) == 0
    dv = s.str.extract(r'-(\d)(\d)').astype(np.uint8,errors="ignore")
    dig_1 = np.array(np.arange(10,1,-1),dtype=np.uint8)
    mult1 = np.sum(np.multiply(digitos,dig_1),axis=1).astype(np.uint16)
    res1 = np.remainder(10*mult1,11).astype(np.uint8)
    res1[res1>=10] = 0
    digitos =  pd.concat([digitos,res1],axis=1)
    dig_2 = np.array(np.arange(11,1,-1),dtype=np.uint8)
    mult2 = np.sum(np.multiply(digitos,dig_2),axis=1).astype(np.uint16)
    res2 = np.remainder(10*mult2,11).astype(np.uint8)
    res2[res2>=10] = 0
    digitos =  pd.concat([digitos,res2],axis=1) 
    return ((dv[0] - res1) == 0) & ((dv[1] - res2) == 0) & (~all_equal)


# Formata CPF
# Retorna Série com CPFs formatados
def formatar_cpf(s: pd.Series, check_val =_PADRAO_CHECAR_VALIDADE,on_error = _PADRAO_ON_ERROR):
    """
    s => série contendo CPFs
    check_val => Caso True, checa validade dos CPFs formatados. Não retorna erro por padrão
    on_error => Caso on_error == "raise", além de checar a validade retorna erro caso haja CPFs inválidos
    """
    s = s.astype(str)
    s.replace(to_replace="\D", value=r"", regex=True,inplace=True) # Remove caracteres que não são dígitos
    s = s.str.pad(11,"left","0")  # Faz padding left com zeros
    s.replace(to_replace="(\d{3})(\d{3})(\d{3})(\d{2})", value=r"\1.\2.\3-\4", regex=True,inplace=True) # Formata
    
    # Checa validade
    if check_val or on_error == "raise":
        invalidos  = s[~validar_cpf(s)]
        quant_errors = invalidos.count()
        print(f"{quant_errors} CPFs inválidos:")
        display(invalidos)
    if on_error == "raise" and quant_errors > 0:
        raise ValueError("Há CPFs inválidos na série.")
    
    return s


# Cria parquet de arquivos .xlsx .csv e json para leitura mais rápida
# Checa intersecção de valores em alguma coluna
# Junta arquivos em um dataframe
def sheet_to_parquet(file_paths:list,colunas:list,parquet_folder:str=False,colunas_sem_repetir:list=False,ignore_parquets=False):
    """
    file_paths => lista de arquivos a serem lidos
    colunas => colunas que serão lidas em todos os arquivos. Assume-se que todos arquivos possuem as mesmas colunas
    parquet_folder => pasta onde serão salvos os parquets. Caso omisso, será criada uma pasta ./Parquets na mesma pasta em que cada arquivo foi lido
    colunas_sem_repetir => Checa para essas colunas se há valores que existe em mais de um dos arquivos
    ignore_parquets => Caso True, independentemente se os parquets tenham sido criados, o arquivo original será lido e um novo parquet será criado
    """
    df = pd.DataFrame(columns=colunas)
    for file_path in file_paths:
        parquet_criado = False
        file_name,extension = os.path.split(file_path)[1].split(".")
        if not parquet_folder:
            parquet_folder = f"{os.path.split(file_path)[0]}/Parquets/"
        parquet_file_path= f"{parquet_folder}{file_name}.gzip"
        if os.path.exists(parquet_file_path) and not ignore_parquets:
            print(f"Lendo arquivo: {parquet_file_path}")
            df_dummy = pd.read_parquet(parquet_file_path)
            parquet_criado = True
        else: 
            print(f"Lendo arquivo: {file_path}")
            if extension == "xlsx":
                df_dummy = pd.read_excel(file_path)[colunas]
            elif extension == "csv":
                df_dummy = pd.read_csv(file_path)[colunas]
            elif extension == "json":
                df_dummy = pd.read_json(file_path)[colunas]
            else:
                raise ValueError(f"Extensão não suportada: {extension}")
            print(parquet_folder)
            if not os.path.exists(parquet_folder):
                print(f"Criando diretório: {parquet_folder}")
                os.mkdir(parquet_folder)
            else:
                print(f"Salvando em diretório: {parquet_folder}")
        if colunas_sem_repetir:
            for coluna_sem_repetir in colunas_sem_repetir:  
                valores_repetidos = np.intersect1d(df[coluna_sem_repetir].unique(), df_dummy[coluna_sem_repetir].unique())
                if len(valores_repetidos) > 0:
                    raise ValueError(f"No arquivo {file_path} na coluna {coluna_sem_repetir} há valores que também estão em outro arquvio: {', '.join(valores_repetidos)}.")
        if not parquet_criado:
            print(f"Criando arquivo: {parquet_file_path}")
            df_dummy.to_parquet(parquet_file_path)
        df = pd.concat([df,df_dummy],ignore_index=True)
    return df


# Valida CNPJ. Assume-se que o CNPJ já está formatado corretamente.
# Caso não esteja formate antes, usando a função formatar_cnpj().
# Retorna série booleana. True => válido, False = inválido
def validar_cnpj(s: pd.Series):
    s = s.astype(str)
    digitos = s.str.extract(r'(\d)(\d).(\d)(\d)(\d).(\d)(\d)(\d)/(\d)(\d)(\d)(\d)-').astype(np.uint8,errors="ignore")
    dv = s.str.extract(r'-(\d)(\d)').astype(np.uint8,errors="ignore")
    dig_1 = np.array([6,7,8,9,2,3,4,5,6,7,8,9 ],dtype=np.uint8)
    mult1 = np.sum(np.multiply(digitos,dig_1),axis=1).astype(np.uint16)
    res1 = np.remainder(mult1,11).astype(np.uint8)
    res1[res1==10] = 0
    digitos =  pd.concat([digitos,res1],axis=1)
    dig_2 = np.array([5,6,7,8,9,2,3,4,5,6,7,8,9],dtype=np.uint8)
    mult2 = np.sum(np.multiply(digitos,dig_2),axis=1).astype(np.uint16)
    res2 = np.remainder(mult2,11).astype(np.uint8)
    res2[res2==10] = 0
    digitos =  pd.concat([digitos,res2],axis=1)
    return ((dv[0] - res1) == 0) & ((dv[1] - res2) == 0)


# Formata CNPJ
# Retorna Série com CNPJS formatados
def formatar_cnpj(s: pd.Series, check_val =_PADRAO_CHECAR_VALIDADE,on_error = _PADRAO_ON_ERROR):
    """
    s => série contendo CNPJs
    check_val => Caso True, checa validade dos CNPJs formatados. Não retorna erro por padrão
    on_error => Caso on_error == "raise", além de checar a validade retorna erro caso haja CNPJs inválidos
    """
    s = s.astype(str)
    s.replace(to_replace="\D", value=r"", regex=True,inplace=True) # Remove caracteres que não são dígitos
    s = s.str.pad(14,"left","0")  # Faz padding left com zeros
    s.replace(to_replace="(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})", value=r"\1.\2.\3/\4-\5", regex=True,inplace=True) # Formata
    
    # Checa validade
    if check_val or on_error == "raise":
        invalidos  = s[~validar_cnpj(s)]
        quant_errors = invalidos.count()
        print(f"{quant_errors} CNPJs inválidos:")
        display(invalidos)
    if on_error == "raise" and quant_errors > 0:
        raise ValueError("Há CNPJs inválidos na série.")
    
    return s


