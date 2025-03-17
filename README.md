MATCHED ANALYTICS (ma)
====================

Tools to track renewable energy supply from public data.

Further details at https://matched.energy.


Setup
----------
The project is built as package 'ma'.

Using [PDM](https://pdm-project.org), or similar:

    gh repo clone matched-energy/matched-analytics   # or simliar
    cp template.env .env                             # edit credentials in .env
    pdm install                                      # instantiate venv and build package
    eval $(pdm venv activate)                        # now `import ma` ... 
    pdm run tests


