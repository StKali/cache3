import sys
from pathlib import Path
project_path: Path = Path(__file__).parents[2].resolve() / 'src'
sys.path.insert(0, str(project_path))

import cache3

release = cache3.__version__
project = cache3.__name__
author = cache3.__author__
copyright = f'2022, {author}'

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
]

templates_path = ['_templates']
exclude_patterns = [
]

# html_theme = 'alabaster'
html_theme = 'furo'

html_theme_options = {
    "light_logo": "logo.png",
    "dark_logo": "logo.png",

    "light_css_variables": {
        "color-brand-primary": "#7C4DFF",
        "color-brand-content": "#7C4DFF",
    },
}

html_static_path = ['_static']

html_sidebars = {
    "**": [
        "sidebar/scroll-start.html",
        "sidebar/brand.html",
        "my_sidebar.html",
        "sidebar/search.html",
        "sidebar/navigation.html",
        "sidebar/ethical-ads.html",
        "sidebar/scroll-end.html",
    ]
}
