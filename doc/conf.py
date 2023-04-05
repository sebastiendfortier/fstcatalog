# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('..'))



# -- Project information -----------------------------------------------------

project = 'fstcatalog'
copyright = '2023, ECCC'
author = 'Sébastien Fortier'

# # The full version, including alpha/beta/rc tags
with open('../VERSION', encoding='utf-8') as f:
    version = f.read()
release = version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
#       'sphinx.ext.coverage',
#extensions = ['sphinx.ext.autodoc', 'sphinx.ext.napoleon']
# nbsphinx for jupyter notebooks
# sphinx.ext.viewcode for code links
# myst_parser for markdown
extensions = ['sphinx.ext.napoleon',
              'sphinx.ext.doctest',
              'sphinx_autodoc_typehints',
              'sphinx_gallery.gen_gallery',
              'nbsphinx',
              'sphinx.ext.viewcode',
              'myst_parser']

napoleon_include_private_with_doc = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['build', '_build', 'Thumbs.db', '.DS_Store']


#options for gallery
sphinx_gallery_conf = {
     'examples_dirs': '../examples',   # path to your example scripts
     'gallery_dirs': 'auto_examples',  # path where to save gallery generated examples
}


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"
html_theme_path = ["_themes", ]


# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_logo = "fstcatalog_logo.png"
html_theme_options = {
    'logo_only': True,
    'display_version': False,
    'vcs_pageview_mode': 'blob'
}

html_context = {
    "display_gitlab": True, # Integrate Gitlab
    "gitlab_host": "gitlab.science.gc.ca",
    "gitlab_user": "CMDS", # Username
    "gitlab_repo": "fstcatalog", # Repo name
    "gitlab_version": "master", # Version
    "conf_py_path": "/doc/", # Path in the checkout to the docs root
}

