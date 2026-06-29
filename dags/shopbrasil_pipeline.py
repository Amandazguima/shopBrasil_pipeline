"""
Atividade 01 - Pipeline ShopBrasil (Airflow + FakeStore API + PostgreSQL)

Topologia (requisito do enunciado):
    [Buscar produtos] -> [Validar schema] -> [Calcular metricas] -> [Persistir]
        linear                linear              fan-out               fan-in

Schedule: todos os dias as 06:00 (America/Sao_Paulo), catchup=False.
"""

from __future__ import annotations

import logging

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

LOG = logging.getLogger(__name__)

FAKESTORE_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "postgres_lab"
SNAPSHOT_TABLE = "shopbrasil_snapshot"
HISTORICO_TABLE = "shopbrasil_historico"


# ---------------------------------------------------------------------------
# Callbacks de ciclo de vida (requisito obrigatorio: on_failure/on_retry/on_success)
# ---------------------------------------------------------------------------
def _log_event(label: str):
    def _cb(context):
        ti = context.get("ti")
        LOG.warning(
            "[%s] task=%s run_id=%s try=%s",
            label,
            ti.task_id if ti else "?",
            context.get("run_id"),
            context.get("try_number"),
        )
    return _cb


CALLBACKS = {
    "on_failure_callback": _log_event("FAIL"),
    "on_retry_callback":  _log_event("RETRY"),
    "on_success_callback": _log_event("OK"),
}


# ---------------------------------------------------------------------------
# Operador customizado (requisito opcional: BaseOperator para validar schema)
# ---------------------------------------------------------------------------
class ValidarProdutosOperator(BaseOperator):
    """Valida o schema minimo dos produtos vindos da API."""

    CAMPOS_OBRIGATORIOS = ("id", "title", "price", "category")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        ti = context["ti"]
        produtos = ti.xcom_pull(task_ids="ingestao.buscar_produtos")
        if not produtos:
            raise AirflowFailException("Lista de produtos vazia apos retry.")
        for p in produtos:
            if not isinstance(p, dict):
                raise AirflowFailException(f"Produto invalido (nao-dict): {p!r}")
            for campo in self.CAMPOS_OBRIGATORIOS:
                if campo not in p:
                    raise AirflowFailException(
                        f"Produto id={p.get('id')} sem campo obrigatorio '{campo}'."
                    )
        LOG.info("Validados %d produtos.", len(produtos))
        return produtos


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _postgres() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def _agregar_por_categoria(produtos: list[dict]) -> list[dict]:
    """Reduz a lista de produtos a 1 linha agregada por categoria."""
    agg: dict[str, dict] = {}
    for p in produtos:
        cat = p["category"]
        if cat not in agg:
            agg[cat] = {
                "id_categoria":  hash(cat) & 0x7FFFFFFF,  # id deterministico
                "categoria":     cat,
                "qtd_produtos":  0,
                "_soma":         0.0,
                "preco_minimo":  float("inf"),
                "preco_maximo":  float("-inf"),
            }
        bucket = agg[cat]
        preco = float(p["price"])
        bucket["qtd_produtos"] += 1
        bucket["_soma"] += preco
        if preco < bucket["preco_minimo"]:
            bucket["preco_minimo"] = preco
        if preco > bucket["preco_maximo"]:
            bucket["preco_maximo"] = preco

    return [{
        "id_categoria": b["id_categoria"],
        "categoria":    b["categoria"],
        "qtd_produtos": b["qtd_produtos"],
        "preco_medio":  round(b["_soma"] / b["qtd_produtos"], 2),
        "preco_minimo": b["preco_minimo"],
        "preco_maximo": b["preco_maximo"],
    } for b in agg.values()]


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="shopbrasil_pipeline",
    description="Pipeline diario de metricas de produtos por categoria (FakeStore -> Postgres).",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["shopbrasil", "ecommerce", "atividade01"],
    default_args={
        "owner": "tech-lead-shopbrasil",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=10),
        **CALLBACKS,
    },
    sla_miss_callback=_log_event("SLA_MISS"),
)
def shopbrasil_pipeline():

    # Operador customizado (requisito opcional): instanciado no escopo do DAG
    # para evitar problema de acesso a tasks via atributo do TaskGroup.
    validar_op = ValidarProdutosOperator(task_id="ingestao.validar_produtos")

    # ============================================================
    # TaskGroup INGESTAO (topologia linear)
    # ============================================================
    with TaskGroup(group_id="ingestao") as ingestao:

        @task(
            task_id="buscar_produtos",
            pool="ecommerce_pool",
            execution_timeout=pendulum.duration(minutes=5),
        )
        def buscar_produtos() -> list[dict]:
            try:
                r = requests.get(FAKESTORE_URL, timeout=20)
                r.raise_for_status()
                produtos = r.json()
            except (requests.RequestException, ValueError) as exc:
                # raise dispara retry com exponential backoff (default_args)
                raise AirflowFailException(f"Falha ao consultar FakeStore API: {exc}") from exc

            if not isinstance(produtos, list) or not produtos:
                raise AirflowFailException("API retornou payload vazio/inesperado.")

            Variable.set("shopbrasil_ultima_qtd", len(produtos))
            LOG.info("API retornou %d produtos.", len(produtos))
            return produtos

        # Dependencia entre buscar_produtos (TaskFlow) e validar_op (BaseOperator).
        # O operador custom NAO fica dentro do with TaskGroup — ele recebe
        # task_id fully-qualified ("ingestao.validar_produtos") para aparecer
        # visualmente dentro do group na UI.
        buscar_produtos() >> validar_op

    # ============================================================
    # TaskGroup ANALISE (fan-out por categoria + fan-in no persist)
    # IMPORTANTE: todas as chamadas de tasks devem acontecer DENTRO do
    # `with`, senao o Airflow cria tasks duplicadas SEM o prefixo do group.
    # ============================================================
    with TaskGroup(group_id="analise") as analise:

        @task(task_id="agregar_por_categoria")
        def agregar(produtos: list[dict]) -> list[dict]:
            agregado = _agregar_por_categoria(produtos)
            LOG.info("Agregadas %d categorias.", len(agregado))
            return agregado

        @task(task_id="calcular_metricas_categoria", pool="ecommerce_pool")
        def calcular_metricas(linha_categoria: dict) -> dict:
            # Fan-out: 1 chamada por categoria via .expand
            # Job minimo: log + retorno (agregacao ja foi feita em serie).
            LOG.info(
                "Categoria '%s' (%d produtos) precos min=%.2f max=%.2f med=%.2f",
                linha_categoria["categoria"],
                linha_categoria["qtd_produtos"],
                linha_categoria["preco_minimo"],
                linha_categoria["preco_maximo"],
                linha_categoria["preco_medio"],
            )
            return linha_categoria

        @task(task_id="consolidar_metricas")
        def consolidar(metricas: list[dict]) -> list[dict]:
            # Fan-in: recebe todas as linhas via XCom automatico
            LOG.info("Consolidadas %d categorias.", len(metricas))
            return metricas

        @task(task_id="persistir_postgres")
        def persistir(metricas: list[dict], produtos: list[dict], **context) -> dict:
            """Snapshot idempotente + historico append-only."""
            # data_interval_end = janela logica da run (data em que os dados se referem).
            # Usar isso em vez de pendulum.now() garante que backfills/catchup
            # gravem com a data correta da janela (e nao a data de hoje).
            data_exec = context["data_interval_end"].in_timezone("America/Sao_Paulo").to_date_string()
            pg = _postgres()

            # 1) Snapshot idempotente (UPSERT por PK categoria+data)
            pg.insert_rows(
                table=SNAPSHOT_TABLE,
                rows=[(
                    m["id_categoria"], m["categoria"], m["qtd_produtos"],
                    m["preco_medio"],  m["preco_minimo"], m["preco_maximo"],
                    data_exec,
                ) for m in metricas],
                target_fields=[
                    "id_categoria", "categoria", "qtd_produtos",
                    "preco_medio", "preco_minimo", "preco_maximo", "data_execucao",
                ],
                replace=True,
                replace_index=["id_categoria", "data_execucao"],
            )

            # 2) Historico append-only (evolucao de precos)
            pg.insert_rows(
                table=HISTORICO_TABLE,
                rows=[(
                    p["id"], p["category"], p["title"],
                    float(p["price"]), data_exec,
                ) for p in produtos],
                target_fields=[
                    "id_produto", "categoria", "titulo", "preco", "data_execucao",
                ],
            )

            LOG.info("Persistidos snapshot (%d) e historico (%d).",
                     len(metricas), len(produtos))
            return {"categorias": len(metricas), "produtos": len(produtos)}

        # Dependencias DENTRO do `with` para que o prefixo `analise.` seja aplicado.
        # validar_op.output e o ponto de entrada vindo do TaskGroup ingestao.
        produtos_ok = validar_op.output
        agregado = agregar(produtos_ok)
        metricas = calcular_metricas.expand(linha_categoria=agregado)
        consolidadas = consolidar(metricas)
        persistir(consolidadas, produtos_ok)


shopbrasil_pipeline()