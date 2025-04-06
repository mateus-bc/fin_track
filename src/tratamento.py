import pandas as pd
import numpy as np
from pathlib import Path



class TratamentoNu:
    def __init__(self,
                 input_path: str):
        
        self.depara_desc0 = {'Transferência recebida pelo Pix ':'NU - PIX Recebido',
         'Pagamento de fatura':"NU - Fatura",
         'Pagamento de boleto efetuado ':'NU - Boleto',
         'Transferência enviada pelo Pix ': 'NU - PIX Enviado',       
         'Resgate RDB':'Reserva - Saque',
         'Aplicação RDB':'Reserva - Depósito',
         'Compra no débito ':'NU - Débito',
         'Compra no débito via NuPay ':'NU - Débito'
         }
        self.input_path  = input_path

    def carregar_fatura(self, caminho_arquivo):
        return pd.read_csv(caminho_arquivo)

    def gerar_coluna_data(self, col):

        dia = col.str[0:2]
        mes = col.str[3:5]
        ano = col.str[6:]

        return pd.to_datetime(ano + '-' + mes + '-' + dia)

    def tratar_fatura(self, df_fatura):
        nu = df_fatura

        # Cols desc
        nu[[f'desc_{i}' for i in range(0,7)]] = nu['Descrição'].str.split('-',expand=True)

        # Col Transação
        nu['Tipo'] = nu['desc_0'].map(self.depara_desc0).fillna('Outros')
        nu['Data'] = self.gerar_coluna_data(nu['Data'])


        # cols Flags
        nu['flag_receita'] = nu['Valor'] > 0
        nu['flag_fatura_xp'] = nu['desc_1'].str.contains('BANCO XP')
        nu['flag_salario'] = (nu['desc_1'].str[1:7] == 'MATEUS')&(nu['Tipo'] == 'PIX - Recebido') 
        nu['flag_aluguel'] = nu['desc_1'].str.contains('Illios')
        
        return nu 
    
    def gerar_base_unificada(self, file_list):
        list_nu = []
        for file_name in file_list:
            link = f'{self.input_path}/{file_name}'
            nu_in = self.carregar_fatura(link)
            nu = self.tratar_fatura(nu_in)
            list_nu.append(nu)
        
        nu_full = pd.concat(list_nu)
        
        cols_out = ['Data','Valor','Tipo','Descrição','flag_receita','flag_fatura_xp','flag_salario','flag_aluguel']
        
        return nu_full[cols_out]

class TratamentoXP:
    def __init__(self,
                 input_path: str):
        self.input_path = input_path

    def carregar_fatura(self, caminho_arquivo):
        return pd.read_csv(caminho_arquivo,sep=';')

    
    def gerar_base_unificada(self, file_list):
        list_xp = []
        for file_name in file_list:
            link = f'{self.input_path}/{file_name}'
            xp_in = self.carregar_fatura(link)
            xp = self.tratar_fatura(xp_in)
            list_xp.append(xp)
        
        xp_full = pd.concat(list_xp)
        
        #cols_out = ['Data','Valor','Tipo','Descrição','flag_gasto','flag_salario']
        
        return xp_full#[cols_out]

    def gerar_coluna_valor(self, col):
        return col.str.replace("R\$ ", "", regex=True)\
            .str.replace(".", "", regex=False)\
            .str.replace(",", ".", regex=True).astype(float)*(-1)
            
 
    def gerar_coluna_data(self, col):

        dia = col.str[0:2].astype(int).clip(upper=28).astype(str)
        mes = col.str[3:5].value_counts().index[0]
        ano = col.str[6:].value_counts().index[0]

        return pd.to_datetime(ano + '-' + mes + '-' + dia)

        
    def tratar_fatura(self, df_fatura):
        xp = df_fatura

        xp['flag_parcela'] = (xp['Parcela'] != '-')
        xp['Tipo'] = np.where(xp['flag_parcela'],'XP - Parcelado','XP - A Vista')
        xp['Descrição'] = xp['Estabelecimento']
        xp['Valor'] = self.gerar_coluna_valor(xp['Valor'])
        xp['Data'] = self.gerar_coluna_data(xp['Data'])

        
        cols_out = ['Data','Valor','Tipo','Descrição','flag_parcela']
        return xp[cols_out]
   
class Tratamento:
    def __init__(self, 
                 inputs_path = 'dados/input'):
        self.input_names = [arquivo.name for arquivo in Path(inputs_path).iterdir() if arquivo.is_file()]
        self.nu = TratamentoNu(input_path= inputs_path)
        self.xp = TratamentoXP(input_path= inputs_path)

    def tratar_dados(self):
        df_nu = self.nu.gerar_base_unificada([file_name for file_name in self.input_names if 'nu' in file_name ])
        df_xp = self.xp.gerar_base_unificada([file_name for file_name in self.input_names if 'xp' in file_name ])
        
        df = pd.concat([df_nu,df_xp]).fillna(False) 
        
        df['flag_alto_valor'] = np.where(df['flag_aluguel']==True,False, df['Valor'].abs() >= 2000)
        


        self.df = df

    def exportar_dados(self, caminho= 'dados/output/fin_track.csv'):
        print(self.df.head())
        self.df.to_csv(caminho,index=False)
        