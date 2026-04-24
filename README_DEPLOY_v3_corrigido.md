# Correção para Streamlit Cloud

O erro do deploy ocorreu porque `svglib` puxou `pycairo`, que precisa de dependências de sistema não disponíveis no Streamlit Cloud.

## Use este requirements.txt

streamlit
pandas
bcrypt
requests
reportlab
psycopg[binary]
psycopg_pool

## Depois de enviar ao GitHub

1. Remova `svglib` do requirements.txt.
2. Substitua o código pelo `streamlit_app_v3_corrigido.py`.
3. No Streamlit Cloud: Manage app > Clear cache & redeploy.
