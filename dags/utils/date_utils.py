import datetime


def obter_pasta_processamento(data_execucao: datetime.date) -> str:
    """
    Determina o nome da pasta no formato YYYYMM com base na data de execução.

    Regra de negócio:
    - Se a execução ocorrer no dia 1 de qualquer mês, a pasta usada
      deverá ser a do mês anterior.
    - Se a execução ocorrer a partir do dia 2, a pasta usada será a do
      mês atual.

    Args:
        data_execucao: A data de execução (geralmente a `logical_date` do Airflow).

    Returns:
        O nome da pasta no formato 'YYYYMM'.
    """
    if data_execucao.day == 1:
        # Se é o primeiro dia do mês, calculamos o dia anterior para pegar o mês correto.
        data_alvo = data_execucao - datetime.timedelta(days=1)
    else:
        # A partir do segundo dia, usamos o mês da própria data de execução.
        data_alvo = data_execucao

    # Formata a data alvo para o padrão 'YYYYMM' e retorna.
    return data_alvo.strftime("%Y%m")
