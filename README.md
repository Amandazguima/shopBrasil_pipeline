# ShopBrasil — Pipeline Airflow

Pipeline diario em Apache Airflow que consome a [FakeStore API](https://fakestoreapi.com),
calcula metricas de preco por categoria em paralelo (via Dynamic Task Mapping)
e grava o resultado em PostgreSQL de forma idempotente.

## Arquitetura

```
                                ┌──► [calcular categoria 1] ──┐
                                │                            │
[buscar_produtos] ► [validar] ──┼──► [calcular categoria 2] ──┼──► [consolidar] ─► [persistir]
                                │                            │
                                └──► [calcular categoria 3] ──┘
   TaskGroup "ingestao"                TaskGroup "analise" (fan-out → fan-in)

Linear (serie)                Paralelo (Dynamic Mapping)        Consolida + grava
```

## Estrutura do projeto

```
.
├── dags/shopbrasil_pipeline.py   # DAG: TaskFlow + TaskGroups + Dynamic Mapping
├── sql/init.sql                  # Schema do banco destino (snapshot + historico)
├── docker-compose.yml            # Airflow 2.9.3 + Postgres meta + Postgres lab
├── .gitignore
└── README.md                     # este arquivo
```

## Subir o ambiente

```bash
docker compose up -d
docker compose logs -f airflow-init   # aguarda a mensagem "inicializada"
```

| Servico | URL / Porta | Credenciais |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Postgres destino | localhost:5433 | lab / lab123 / labdb |
| Postgres meta (Airflow) | interno:5432 | airflow / airflow |

## DAG `shopbrasil_pipeline`

| Atributo | Valor |
|---|---|
| Schedule | `0 6 * * *` (America/Sao_Paulo) |
| catchup | False |
| Start date | 2026-01-01 |
| Retries | 3, com exponential backoff |
| Pool | `ecommerce_pool` (2 slots) |

**Topologia:**

- `ingestao.buscar_produtos` — coleta 20 produtos da FakeStore API com retry/exponential backoff
- `ingestao.validar_produtos` — operador customizado `ValidarProdutosOperator` valida schema minimo
- `analise.agregar_por_categoria` — agrega produtos por categoria em serie
- `analise.calcular_metricas_categoria` — **fan-out** via Dynamic Task Mapping (1 task por categoria, pool `ecommerce_pool`)
- `analise.consolidar_metricas` — **fan-in**, recebe lista consolidada das instancias mapeadas
- `analise.persistir_postgres` — UPSERT no snapshot idempotente + INSERT na tabela de historico

## Persistencia

Duas tabelas no schema `public` do banco `labdb`:

- **`shopbrasil_snapshot`** (idempotente) — chave primaria `(id_categoria, data_execucao)`. Re-rodar a mesma run sobrescreve a linha, nao duplica.
- **`shopbrasil_historico`** (append-only) — chave serial. Cada run adiciona 1 linha por produto para acompanhar evolucao de precos.

## Inspecionar os dados

```bash
# Snapshot diario
docker exec airflow-lab-db psql -U lab -d labdb -c \
  "SELECT categoria, qtd_produtos, preco_medio, preco_minimo, preco_maximo
   FROM shopbrasil_snapshot ORDER BY data_execucao DESC, categoria;"

# Historico de precos por data
docker exec airflow-lab-db psql -U lab -d labdb -c \
  "SELECT data_execucao, COUNT(*) AS linhas, COUNT(DISTINCT id_produto) AS produtos
   FROM shopbrasil_historico GROUP BY 1 ORDER BY 1;"
```

## Disparar e observar

```bash
# Disparar manualmente
docker exec airflow-scheduler airflow dags trigger shopbrasil_pipeline

# Listar runs
docker exec airflow-scheduler airflow dags list-runs -d shopbrasil_pipeline

# Ver task instances de uma run (confirma fan-out)
docker exec airflow-scheduler airflow tasks states-for-dag-run \
  shopbrasil_pipeline manual__<run_id>
```

Na UI: **DAGs → shopbrasil_pipeline → aba Grid**. As 4 tarefas `calcular_metricas_categoria` aparecem empilhadas em paralelo, cada uma com seu `map_index`.