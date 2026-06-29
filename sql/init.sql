-- =============================================================================
-- Atividade 01 — Pipeline ShopBrasil
-- Schema do banco de destino (postgres-lab).
-- Duas tabelas:
--   1) shopbrasil_snapshot  → idempotente (PK por categoria + data_execucao)
--   2) shopbrasil_historico → append-only (acompanha evolucao de precos)
-- =============================================================================

CREATE TABLE IF NOT EXISTS shopbrasil_snapshot (
    id_categoria      INTEGER     NOT NULL,
    categoria         TEXT        NOT NULL,
    qtd_produtos      INTEGER     NOT NULL,
    preco_medio       NUMERIC(12,2) NOT NULL,
    preco_minimo      NUMERIC(12,2) NOT NULL,
    preco_maximo      NUMERIC(12,2) NOT NULL,
    data_execucao     DATE        NOT NULL,
    inserido_em       TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_shopbrasil_snapshot PRIMARY KEY (id_categoria, data_execucao)
);

CREATE TABLE IF NOT EXISTS shopbrasil_historico (
    id                BIGSERIAL   PRIMARY KEY,
    id_produto        INTEGER     NOT NULL,
    categoria         TEXT        NOT NULL,
    titulo            TEXT        NOT NULL,
    preco             NUMERIC(12,2) NOT NULL,
    data_execucao     DATE        NOT NULL,
    inserido_em       TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shopbrasil_historico_categoria_data
    ON shopbrasil_historico (categoria, data_execucao);