### path setup ####################################################################################

from glob import glob
import datetime
import os
import sys

sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('.'))
###################################################################################################
### Project Information ###########################################################################
###################################################################################################

project = 'timex_lca'
copyright = datetime.date.today().strftime("%Y") + ' Timex Developers'
version: str = 'latest' # required by the version switcher

###################################################################################################
### Project Configuration #########################################################################
###################################################################################################

needs_sphinx = '7.0.0'

extensions = [
    # core extensions
    'autoapi.extension',
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    'sphinx.ext.inheritance_diagram',
    'sphinx.ext.napoleon',
    # iPython extensions
    'IPython.sphinxext.ipython_directive',
    'IPython.sphinxext.ipython_console_highlighting',
    # Markdown support
    'myst_nb',
    # 'myst_parser', # do not enable separately if using myst_nb, compare: https://github.com/executablebooks/MyST-NB/issues/421#issuecomment-1164427544
    # responsive web component support
    'sphinx_design',
    # custom 404 page
    'notfound.extension',
    # custom favicons
    'sphinx_favicon',
    # copy button on code blocks
    "sphinx_copybutton",
]

autoapi_dirs = ['../timex_lca']
autoapi_type = 'python'
autoapi_ignore = [
    '*/data/*',
    '*tests/*',
    '*tests.py',
    '*validation.py',
    '*version.py',
    '*.rst',
    '*.yml',
    '*.md',
    '*.json',
    '*.data'
]

autoapi_options = [
    'members',
    'undoc-members',
    'private-members',
    'show-inheritance',
    'show-module-summary',
    #'special-members',
    #'imported-members',
    'show-inheritance-diagram'
]

autoapi_python_class_content = 'both'
autoapi_member_order = 'groupwise'
autoapi_root = 'content/api'
autoapi_keep_files = False


autosummary_generate = True

master_doc = "index"

root_doc = 'index'
html_static_path = ['_static']
templates_path = ['_templates']
exclude_patterns = ['_build']
html_theme = "pydata_sphinx_theme"

suppress_warnings = [
    "myst.header" # suppress warnings of the kind "WARNING: Non-consecutive header level increase; H1 to H3"
]


####################################################################################################
### Theme html Configuration #######################################################################
####################################################################################################

html_show_sphinx = False
html_show_copyright = True

html_css_files = [
    "css/custom.css",
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.1.1/css/all.min.css" # for https://fontawesome.com/ icons
]

html_sidebars = {
    "**": [
        "search-field.html",
        "sidebar-nav-bs.html",
    ],
}

html_theme_options = {
    # page elements
    "navbar_start": ["navbar-logo"],
    "navbar_end": ["navbar-icon-links.html"],
    "navbar_persistent": ["theme-switcher"], # this is where the search button is usually placed
    "footer_start": ["copyright"],
    "footer_end": ["footer"],
    "secondary_sidebar_items": ["page-toc", "edit-this-page", "sourcelink", "support"],
    "header_links_before_dropdown": 7,
    # page elements content
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/TimoDiepers/timex",
            "icon": "fab fa-brands fa-github",
        },
        {
            "name": "Conda",
            "url": "https://anaconda.org/diepers/timex_lca",
            "icon": "fa-brands fa-python",
            "type": "fontawesome",
        },
    ],
    # various settings
    "collapse_navigation": True,
    "show_prev_next": False,
    "use_edit_page_button": True,
    "navigation_with_keys": True,
    "logo": {
        "image_light": "logo_light.svg",
        "image_dark": "logo_dark.svg"
    },
}

# required by html_theme_options: "use_edit_page_button"
html_context = {
    "github_user": "TimoDiepers",
    "github_repo": "timex_lca",
    "github_version": "main",
    "doc_path": "docs",
}

# notfound Configuration ################################################
# https://sphinx-notfound-page.readthedocs.io

notfound_context = {
    'title': 'Page Not Found',
    'body': '''                                                                                                                                           
        <h1>🍂 Page Not Found (404)</h1>
        <p>
        Oops! It looks like you've stumbled upon a page that's been recycled into the digital abyss.
        But don't worry, we're all about sustainability here.
        Why not take a moment to reduce, reuse, and recycle your clicks by heading back to the main page?
        And remember, every little bit counts in the grand scheme of things.
        </p>
    ''',
}

####################################################################################################
### Extension Configuration ########################################################################
####################################################################################################

# myst_parser Configuration ############################################
# https://myst-parser.readthedocs.io/en/latest/configuration.html

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'myst-nb',
    '.ipynb': 'myst-nb'
}


myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "html_image",
]

# myst-nb configuration ################################################
# https://myst-nb.readthedocs.io/en/latest/configuration.html

nb_execution_mode = 'off'

# sphinx-favicon configuration #########################################
# https://github.com/tcmetzger/sphinx-favicon

favicons = [
    {
        "rel": "icon",
        "sizes": "100x100",
        "href": "logo/BW_favicon_100x100.png",
    },
    {
        "rel": "apple-touch-icon",
        "sizes": "500x500",
        "href": "logo/BW_favicon_500x500.png"
    },
]
