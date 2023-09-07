-- Restrições:
-- * 2017-01-01 até 2022-12-31
-- * todas as datas preenchidas (total de 1485 datas distintas)
-- * sem saltos bruscos (prováveis incosistências)
-- * sem grupamentos (para simplificiar a simulação)

with tickers as (
  select codneg as ticker -- 137 codneg distintos
  from `stock_exchange.b3_stock_quotes_daily`
  where data between '2017-01-01' and '2022-12-31'
      AND TPMERC = '010'
      and (right(codneg, 1) in ('3', '4', '5', '6', '7', '8') or right(codneg, 2) in ('11', '32', '33', '34', '35'))
      and fatcot = 1
      and codneg not in ('MEAL3', 'ETER3', 'CVCB3', 'BRAP3', 'PRIO3', 'XPCM11', 'BOBR4', 'PDGR3', 'KEPL3', 'RSID3', 'FHER3', 'OIBR4', 'BRAP4', 'VIVR3', 'GFSA3', 'RCSL4', 'UNIP6', 'CMIG4', 'MGLU3', 'ATOM3', 'SHUL4') -- 21 tickers removidos por apresentarem variações anormais
      and codneg not in ('TCSA3', 'PMAM3', 'FLMA11', 'HBOR3', 'CTXT11', 'ABCP11') -- 6 tickers removidos por possuírem grupamentos
  group by codneg
  having count(data) = 1485
),
adjusts as (
    select
      lastDatePrior,
      isinCode,
      exp(sum(ln(
        case
          when label = 'GRUPAMENTO' then 1 / factor
          else 1 / (1 + factor / 100)
        end
      ))) as factor
    from `stock_exchange.b3_stock_dividends`
    group by lastDatePrior, isinCode
),
b3_returns AS (
SELECT
  DATA as date,
  codneg as ticker,
  preabe as price,
  (
    CASE
      WHEN LEAD(preabe) OVER (PARTITION BY codneg ORDER BY DATA) != 0 AND preabe != 0 THEN LN((LEAD(preabe) OVER (PARTITION BY codneg ORDER BY DATA)) / preabe)
      WHEN preabe != 0
    AND preult !=0 THEN LN(preult/preabe)
    ELSE
    0
    END
  ) AS return_raw,
  (
    CASE
      WHEN LEAD(preabe) OVER (PARTITION BY codneg ORDER BY DATA) != 0 AND preabe != 0 THEN LN((LEAD(preabe) OVER (PARTITION BY codneg ORDER BY DATA)) / (coalesce(adjusts.factor, 1) * preabe))
      WHEN preabe != 0
      AND preult !=0 THEN LN(preult/preabe)
      ELSE
      0
    END
  ) AS return_adjusted,
  coalesce(adjusts.factor, 1) as adjust_factor
FROM
  `stock_exchange.b3_stock_quotes_daily`
    inner join tickers on codneg = tickers.ticker
    left join adjusts on adjusts.isinCode = codisi and adjusts.lastDatePrior = data
WHERE data between '2017-01-01' and '2022-12-31'
)
select
  date,
  ticker,
  price,
  (
    case
      when abs(return_adjusted) < abs(return_raw) then adjust_factor
      else 1
    end
  ) as adjust_factor,
  (
    case
      when abs(return_adjusted) < abs(return_raw) then return_adjusted
      else return_raw
    end
  ) as return,
  coalesce(
    SUM(
        case
        when abs(return_adjusted) < abs(return_raw) then return_adjusted
        else return_raw
        end
    ) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    0
  ) as total
from b3_returns
;