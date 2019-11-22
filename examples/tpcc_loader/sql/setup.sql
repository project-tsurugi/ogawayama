CREATE EXTENSION ogawayama_fdw;

CREATE SERVER ogawayama FOREIGN DATA WRAPPER ogawayama_fdw;

CREATE USER MAPPING FOR PUBLIC SERVER ogawayama;

create foreign table warehouse
(
  w_id       integer,
  w_name     text,
  w_street_1 text,
  w_street_2 text,
  w_city     text,
  w_state    text,
  w_zip      text,
  w_tax      double precision,
  w_ytd      double precision
)
server ogawayama;


create foreign table district
(
  d_id        integer,
  d_w_id      integer,
  d_name      text,
  d_street_1  text,
  d_street_2  text,
  d_city      text,
  d_state     text,
  d_zip       text,
  d_tax       double precision,
  d_ytd       double precision,
  d_next_o_id integer
) server ogawayama;


create foreign table customer
(
  c_id            integer,
  c_d_id          integer,
  c_w_id          integer,
  c_first         text,
  c_middle        text,
  c_last          text,
  c_street_1      text,
  c_street_2      text,
  c_city          text,
  c_state         text,
  c_zip           text,
  c_phone         text,
  c_since         text,
  c_credit        text,
  c_credit_lim    double precision,
  c_discount      double precision,
  c_balance       double precision,
  c_ytd_payment   double precision,
  c_payment_cnt   integer,
  c_delivery_cnt  integer,
  c_data          text
) server ogawayama;


create foreign table customer_secondary
(
  c_d_id          integer,
  c_w_id          integer,
  c_last          text,
  c_first         text,
  c_id            integer
) server ogawayama;


create foreign table new_order
(
  no_o_id  integer,
  no_d_id  integer,
  no_w_id  integer
) server ogawayama;


create foreign table orders
(
  o_id          integer,
  o_d_id        integer,
  o_w_id        integer,
  o_c_id        integer,
  o_entry_d     text,
  o_carrier_id  integer,
  o_ol_cnt      integer,
  o_all_local   integer
) server ogawayama;


create foreign table orders_secondary
(
  o_d_id        integer,
  o_w_id        integer,
  o_c_id        integer,
  o_id          integer
) server ogawayama;


create foreign table order_line
(
  ol_o_id         integer,
  ol_d_id         integer,
  ol_w_id         integer,
  ol_number       integer,
  ol_i_id         integer,
  ol_supply_w_id  integer,
  ol_delivery_d   text,
  ol_quantity     integer,
  ol_amount       double precision,
  ol_dist_info    text
) server ogawayama;


create foreign table item
(
  i_id      integer,
  i_im_id   integer,
  i_name    text,
  i_price   double precision,
  i_data    text
) server ogawayama;


create foreign table stock
(
  s_i_id        integer,
  s_w_id        integer,
  s_quantity    integer,
  s_dist_01     text,
  s_dist_02     text,
  s_dist_03     text,
  s_dist_04     text,
  s_dist_05     text,
  s_dist_06     text,
  s_dist_07     text,
  s_dist_08     text,
  s_dist_09     text,
  s_dist_10     text,
  s_ytd         integer,
  s_order_cnt   integer,
  s_remote_cnt  integer,
  s_data        text
) server ogawayama;
