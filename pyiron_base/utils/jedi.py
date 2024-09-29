# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.


def fix_ipython_autocomplete(enable: bool = True) -> None:
    """Change autocomplete behavior for IPython > 6.x

    Parameters:
        enable (bool): Whether to use the trick. Default is True.

    Notes:
    - Since IPython > 6.x, the `jedi` package is used for autocomplete by default.
    - In some cases, the autocomplete doesn't work correctly (see e.g. `here <https://github.com/ipython/ipython/issues/11653>`_).
    - To set the correct behavior, we should use the following in IPython environment:
        %config Completer.use_jedi = False
    - Alternatively, you can add the following to IPython config (`<HOME>\.ipython\profile_default\ipython_config.py`):
        c.Completer.use_jedi = False
    """

    try:
        __IPYTHON__
    except NameError:
        pass
    else:
        from IPython import __version__

        major = int(__version__.split(".")[0])
        if major >= 6:
            from IPython import get_ipython

            get_ipython().Completer.use_jedi = not enable
