import os
import uuid
import pandas as pd
from flask import (
    Blueprint, render_template, request, redirect, 
    url_for, g, send_from_directory, flash
)
# Importa os serviços que contêm a lógica de negócio
from Services.DRE.ServicoRelatoriosDRE import ServicoRelatoriosDRE
from Services.DRE.ServicoRelatoriosRateio import ServicoRelatoriosRateio
from Config import CAMINHOS_ARQUIVOS

# Define o diretório de downloads (um nível acima de 'Routes', na raiz)
DOWNLOAD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Downloads'))

# Cria um "Blueprint" do Flask. É como um mini-app.
dre_blueprint = Blueprint(
    'dre', 
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), '..', 'Templates')
)

@dre_blueprint.route('/')
def index():
    """
    Renderiza a página HTML do DRE.
    O usuário verá esta página ao acessar http://localhost:5000/dre/
    """
    return render_template('DRE_Rentabilidade.html')

@dre_blueprint.route('/processar', methods=['POST'])
def processar_dre():
    """
    Aciona o processamento. Esta é a rota que o botão "Processar" chama.
    Gera um único arquivo Excel com múltiplas abas.
    """
    # 'g.mapeamentos' foi carregado no App.py pelo @app.before_request
    if g.mapeamentos is None:
        flash("Erro crítico: Mapeamentos não puderam ser carregados do banco.", "error")
        return render_template('DRE_Rentabilidade.html')

    try:
        # 1. Instancia os serviços, passando os mapeamentos e caminhos
        rateio_service = ServicoRelatoriosRateio(g.mapeamentos, CAMINHOS_ARQUIVOS)
        dre_service = ServicoRelatoriosDRE(g.mapeamentos, CAMINHOS_ARQUIVOS)

        # 2. Chama o método 'consolidado' que orquestra tudo
        print("Iniciando processamento consolidado...")
        # O método retorna um dicionário de DataFrames
        relatorios = dre_service.consolidado(rateio_service)
        
        # 3. Salva os relatórios em um único Excel
        # Cria um nome de arquivo único para evitar conflitos
        filename = f"DRE_Rentabilidade_{uuid.uuid4().hex[:8]}.xlsx"
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        
        print(f"Salvando relatórios em: {filepath}")
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            for nome_aba, df in relatorios.items():
                if not df.empty:
                    # Limita o nome da aba a 31 caracteres (limite do Excel)
                    nome_aba_curto = nome_aba.replace("_", " ")[:31]
                    df.to_excel(writer, sheet_name=nome_aba_curto, index=False)
                else:
                    print(f"Aba '{nome_aba}' está vazia, pulando.")
        
        print(f"Processamento concluído. Arquivo gerado: {filename}")
        
        # 4. Redireciona o usuário para a rota de download
        return redirect(url_for('dre.download', filename=filename))

    except Exception as e:
        # Se algo der errado, exibe a mensagem de erro na página
        print(f"ERRO NO PROCESSAMENTO: {e}")
        import traceback
        traceback.print_exc() # Imprime o stack trace completo no console
        # Envia a mensagem de erro para o frontend
        flash(f"Ocorreu um erro: {e}", "error")
        return render_template('DRE_Rentabilidade.html')

@dre_blueprint.route('/download/<path:filename>')
def download(filename):
    """
    Serve o arquivo gerado para o usuário.
    """
    print(f"Tentando enviar o arquivo: {filename} do diretório: {DOWNLOAD_DIR}")
    try:
        return send_from_directory(
            DOWNLOAD_DIR,
            filename,
            as_attachment=True
        )
    except FileNotFoundError:
        flash("Erro: Arquivo não encontrado no servidor.", "error")
        return redirect(url_for('dre.index'))