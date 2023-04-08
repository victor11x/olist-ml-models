-- Databricks notebook source
-- WITH tb_pedidos AS (

--   SELECT 
--       DISTINCT 
--       t1.idPedido,
--       t2.idVendedor

--   FROM silver.olist.pedido AS t1

--   LEFT JOIN silver.olist.item_pedido as t2
--   ON t1.idPedido = t2.idPedido

--   WHERE t1.dtPedido < '2018-01-01'
--   AND t1.dtPedido >= add_months('2018-01-01', -6)
--   AND idVendedor IS NOT NULL

-- ),

-- tb_join AS (

--   SELECT 
--         t1.idVendedor,
--         t2.*         

--   FROM tb_pedidos AS t1

--   LEFT JOIN silver.olist.pagamento_pedido AS t2
--   ON t1.idPedido = t2.idPedido

-- ),

-- tb_group AS (

--   SELECT idVendedor,
--          descTipoPagamento,
--          count(distinct idPedido) as qtdePedidoMeioPagamento,
--          sum(vlPagamento) as vlPedidoMeioPagamento

--   FROM tb_join

--   GROUP BY idVendedor, descTipoPagamento
--   ORDER BY idVendedor, descTipoPagamento

-- ),

-- tb_summary AS (

--   SELECT 
--     idVendedor,

--     sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido,
--     sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card_pedido,
--     sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido,
--     sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido,

--     sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido,
--     sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido,
--     sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido,
--     sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido,

--     sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido,
--     sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_credit_card_pedido,
--     sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_voucher_pedido,
--     sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_debit_card_pedido,

--     sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
--     sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
--     sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
--     sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido

--   FROM tb_group

--   GROUP BY idVendedor

-- ),

-- tb_cartao as (

--   SELECT idVendedor,
--          AVG(nrParcelas) AS avgQtdeParcelas,
--          PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas,
--          MAX(nrParcelas) AS maxQtdeParcelas,
--          MIN(nrParcelas) AS minQtdeParcelas

--   FROM tb_join

--   WHERE descTipoPagamento = 'credit_card'

--   GROUP BY idVendedor

-- )

-- SELECT 
--        '2018-01-01' AS dtReference,
--        t1.*,
--        t2.avgQtdeParcelas,
--        t2.medianQtdeParcelas,
--        t2.maxQtdeParcelas,
--        t2.minQtdeParcelas

-- FROM tb_summary as t1

-- LEFT JOIN tb_cartao as t2
-- ON t1.idVendedor = t2.idVendedor

-- COMMAND ----------

with tb_join as (
select t2.*,t3.idVendedor

from silver.olist.pedido as t1

left join silver.olist.pagamento_pedido as t2
on t1.idPedido = t2.idPedido

left join silver.olist.item_pedido as t3
on t1.idpedido = t3.idpedido

where
dtPedido < '2018-01-01' and dtpedido >= add_months('2018-01-01',-6)
and t3.idVendedor is not null
), 

tb_group as (
select 
idVendedor,
descTipoPagamento,count(distinct idPedido) as qtdPedidoMeioPagamento,
sum(vlPagamento) as vlPedidoMeioPagamento

from tb_join

group by 1,2
order by 1,2)

select  
idVendedor,
--quantidade de pedidos por tipo de pagamento:
sum(case when descTipoPagamento = 'idVendedor'  then qtdPedidoMeioPagamento else 0 end )  as qtd_boleto_pedido,
sum(case when descTipoPagamento = 'boleto'      then qtdPedidoMeioPagamento else 0 end )  as qtd_voucher_pedido,
sum(case when descTipoPagamento = 'credit_card' then qtdPedidoMeioPagamento else 0 end )  as qtd_credit_card_pedido,
sum(case when descTipoPagamento = 'voucher'     then qtdPedidoMeioPagamento else 0 end )  as qtd_voucher_pedido,
sum(case when descTipoPagamento = 'debit_card'  then qtdPedidoMeioPagamento else 0 end )  as qtd_debit_card_pedido,

--valor de pedidos por tipo de pagamento:
sum(case when descTipoPagamento = 'idVendedor'  then vlPedidoMeioPagamento else 0 end )   as valor_boleto_pedido,
sum(case when descTipoPagamento = 'boleto'      then vlPedidoMeioPagamento else 0 end )   as valor_voucher_pedido,
sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end )   as valor_credit_card_pedido,
sum(case when descTipoPagamento = 'voucher'     then vlPedidoMeioPagamento else 0 end )   as valor_voucher_pedido,
sum(case when descTipoPagamento = 'debit_card'  then vlPedidoMeioPagamento else 0 end )   as valor_debit_card_pedido,

--porcentagem de pedidos por tipo de pagamento:
sum(case when descTipoPagamento = 'idVendedor'  then qtdPedidoMeioPagamento else 0 end )  / sum(qtdPedidoMeioPagamento)   as pct_qtd_boleto_pedido,
sum(case when descTipoPagamento = 'boleto'      then qtdPedidoMeioPagamento else 0 end )  / sum(qtdPedidoMeioPagamento)   as pct_qtd_voucher_pedido,
sum(case when descTipoPagamento = 'credit_card' then qtdPedidoMeioPagamento else 0 end )  / sum(qtdPedidoMeioPagamento)   as pct_qtd_credit_card_pedido,
sum(case when descTipoPagamento = 'voucher'     then qtdPedidoMeioPagamento else 0 end )  / sum(qtdPedidoMeioPagamento)   as pct_qtd_voucher_pedido,
sum(case when descTipoPagamento = 'debit_card'  then qtdPedidoMeioPagamento else 0 end )  / sum(qtdPedidoMeioPagamento)   as pct_qtd_debit_card_pedido,

--porcentagem do valor de pedidos por tipo de pagamento:
sum(case when descTipoPagamento = 'boleto'      then vlPedidoMeioPagamento else 0 end )   / sum(vlPedidoMeioPagamento)    as pct_valor_voucher_pedido,
sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end )   / sum(vlPedidoMeioPagamento)    as pct_valor_credit_card_pedido,
sum(case when descTipoPagamento = 'voucher'     then vlPedidoMeioPagamento else 0 end )   / sum(vlPedidoMeioPagamento)    as pct_valor_voucher_pedido,
sum(case when descTipoPagamento = 'debit_card'  then vlPedidoMeioPagamento else 0 end )   / sum(vlPedidoMeioPagamento)    as pct_valor_debit_card_pedido


from tb_group
group by 1

-- COMMAND ----------


