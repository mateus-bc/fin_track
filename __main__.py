from src.tratamento import Tratamento
import warnings

warnings.filterwarnings('ignore')


tratamento = Tratamento(inputs_path = 'C:/Users/mband/Google Drive/FIN_dados')
tratamento.tratar_dados()
tratamento.exportar_dados()

print('Fin_track exportado!')