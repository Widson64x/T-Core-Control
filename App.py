import os
from flask import Flask, render_template, g, flash, redirect, url_for
from dotenv import load_dotenv
from Routes.DRE_Rentabilidade import dre_blueprint
from Routes.Menu import menu_blueprint
from Db.Connection import Carregar_Mapeamento_Banco

# Carregar variáveis de ambiente (do arquivo .env)
load_dotenv()

# --- Esta é a sua nova base para a aplicação ---
# O prefixo DEVE começar com uma barra "/"
BASE_PREFIX = "/System"

app = Flask(
    __name__,
    static_folder="static",
    # O caminho estático agora usa o prefixo corrigido
    static_url_path=f"{BASE_PREFIX}/static",
)

# Configura uma chave secreta para sessões (necessário para flash messages)
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY", "uma-chave-secreta-muito-forte")

# --- Registrar Blueprints (Rotas) ---
# A rota 'menu.index' ('/') será registrada em '/System/'
app.register_blueprint(menu_blueprint, url_prefix=BASE_PREFIX)
# A rota 'dre.index' ('/') será registrada em '/System/Dre/'
app.register_blueprint(dre_blueprint, url_prefix=f"{BASE_PREFIX}/Dre")


# --- Rotas Globais ---
@app.route('/')
def index_redirect():
    """
    A rota raiz '/' agora apenas redireciona
    para sua nova página de menu principal em /System/
    """
    return redirect(url_for('menu.index'))

@app.before_request
def load_mappings_into_g():
    """
    Carrega os mapeamentos do banco ANTES de cada requisição
    e armazena no objeto 'g' do Flask.
    
    A função Carregar_Mapeamento_Banco() usa cache,
    então o banco só será consultado de 1 em 1 hora.
    """
    try:
        # 'g' é um objeto global por requisição do Flask
        if 'mapeamentos' not in g:
            mapeamentos = Carregar_Mapeamento_Banco()
            if mapeamentos is None:
                raise Exception("Falha ao carregar mapeamentos do banco.")
            g.mapeamentos = mapeamentos
    except Exception as e:
        # Em um app real, isso renderizaria uma página de erro
        print(f"ERRO CRÍTICO ao carregar mapeamentos: {e}")
        g.mapeamentos = None
        flash(f"ERRO CRÍTICO DE BANCO: Não foi possível carregar os mapeamentos. {e}", "error")


if __name__ == '__main__':
    # Cria a pasta 'downloads' se ela não existir
    download_dir = os.path.join(os.path.dirname(__file__), 'Downloads')
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    # Inicia o servidor
    # host='0.0.0.0' permite acesso de outras máquinas na rede
    app.run(debug=True, host='0.0.0.0', port=5000)