select
    l_supplierkey,
    l_returnflag,
    l_status,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
  from
    lineitem
  where
    l_returnflag in ('A','R')
  group by
    l_returnflag,
    l_status,
    l_supplierkey