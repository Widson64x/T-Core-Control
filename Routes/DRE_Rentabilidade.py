import os
import uuid
import pandas as pd
from flask import (
    Blueprint, render_template, request, redirect, 
    url_for, g, send_from_directory, flash
)
# Importa as classes de serviço que contêm a "inteligência" do processamento
from Services.DRE.ServicoRelatoriosDRE import ServicoRelatoriosDRE
from Services.DRE.ServicoRelatoriosRateio import ServicoRelatoriosRateio
from Config import CAMINHOS_ARQUIVOS

# Define onde os arquivos gerados serão salvos para o usuário baixar depois
DOWNLOAD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Downloads'))

# Cria o Blueprint (um módulo de rotas do Flask)
dre_blueprint = Blueprint(
    'dre', 
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), '..', 'Templates')
)

@dre_blueprint.route('/')
def index():
    """
    Rota principal: Exibe a tela HTML (o formulário com o botão).
    """
    return render_template('DRE_Rentabilidade.html')

@dre_blueprint.route('/processar', methods=['POST'])
def processar_dre():
    """
    Rota de Ação: Ocorre quando o usuário clica em "Processar".
    Aqui acontece a mágica de instanciar os serviços e gerar o Excel.
    """
    # 'g' é uma variável global do Flask válida apenas durante a requisição.
    # g.mapeamentos foi preenchido no App.py (middleware) para evitar recarregar o banco toda hora.
    if g.mapeamentos is None:
        flash("Erro crítico: Mapeamentos não puderam ser carregados do banco.", "error")
        return render_template('DRE_Rentabilidade.html')

    try:
        # 1. Instancia os serviços
        # Passamos os mapeamentos (dados do banco) e os caminhos (configuração de onde estão os Excels)
        rateio_service = ServicoRelatoriosRateio(g.mapeamentos, CAMINHOS_ARQUIVOS)
        dre_service = ServicoRelatoriosDRE(g.mapeamentos, CAMINHOS_ARQUIVOS)

        # 2. Executa a lógica principal
        print("Iniciando processamento consolidado...")
        # O método .consolidado() chama todas as funções internas e retorna um dicionário
        # onde a Chave é o nome da Aba e o Valor é o DataFrame pronto.
        relatorios = dre_service.consolidado(rateio_service)
        
        # 3. Salva o arquivo físico
        # Gera um nome aleatório (uuid) para evitar que dois usuários sobrescrevam o arquivo um do outro
        filename = f"DRE_Rentabilidade_{uuid.uuid4().hex[:8]}.xlsx"
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        
        print(f"Salvando relatórios em: {filepath}")
        
        # Usa o ExcelWriter para criar um arquivo com múltiplas abas
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            for nome_aba, df in relatorios.items():
                if not df.empty:
                    # O Excel limita nomes de aba a 31 caracteres, cortamos se for maior
                    nome_aba_curto = nome_aba.replace("_", " ")[:31]
                    df.to_excel(writer, sheet_name=nome_aba_curto, index=False)
                else:
                    print(f"Aba '{nome_aba}' está vazia, pulando.")
        
        print(f"Processamento concluído. Arquivo gerado: {filename}")
        
        # 4. Redireciona o navegador para a rota de download, passando o nome do arquivo gerado
        return redirect(url_for('dre.download', filename=filename))

    except Exception as e:
        # Captura qualquer erro (arquivo não encontrado, coluna errada, banco fora)
        print(f"ERRO NO PROCESSAMENTO: {e}")
        import traceback
        traceback.print_exc() # Mostra o erro detalhado no terminal
        # flash() envia a mensagem de erro para aparecer no topo do HTML
        flash(f"Ocorreu um erro: {e}", "error")
        return render_template('DRE_Rentabilidade.html')

@dre_blueprint.route('/download/<path:filename>')
def download(filename):
    """
    Rota de Download: Entrega o arquivo físico para o browser do usuário.
    """
    print(f"Tentando enviar o arquivo: {filename} do diretório: {DOWNLOAD_DIR}")
    try:
        # send_from_directory é uma função segura do Flask para enviar arquivos
        return send_from_directory(
            DOWNLOAD_DIR,
            filename,
            as_attachment=True # Força o download ao invés de tentar abrir no navegador
        )
    except FileNotFoundError:
        flash("Erro: Arquivo não encontrado no servidor.", "error")
        return redirect(url_for('dre.index'))