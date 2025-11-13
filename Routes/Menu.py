import os
from flask import Blueprint, render_template

# Define o Blueprint
# O template_folder aponta para a pasta 'Templates' um nível acima
menu_blueprint = Blueprint(
    'menu', 
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), '..', 'Templates')
)

@menu_blueprint.route('/')
def index():
    """
    Renderiza a nova página de Menu principal.
    Esta será a página inicial (homepage) da sua aplicação.
    """
    return render_template('Menu.html')