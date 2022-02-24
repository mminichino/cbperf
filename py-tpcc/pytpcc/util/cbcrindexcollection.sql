drop index CUSTOMER.CU_ID_D_ID_W_ID USING GSI
drop index CUSTOMER.CU_W_ID_D_ID_LAST USING GSI
drop index DISTRICT.DI_ID_W_ID using gsi
drop index ITEM.IT_ID using gsi
drop index NEW_ORDER.NO_D_ID_W_ID using gsi
drop index ORDERS.OR_O_ID_D_ID_W_ID using gsi
drop index ORDERS.OR_W_ID_D_ID_C_ID using gsi
drop index ORDER_LINE.OL_O_ID_D_ID_W_ID using gsi
drop index STOCK.ST_W_ID_I_ID1 using gsi
drop index DISTRICT.D_ID using gsi
drop index CUSTOMER.C_ID using gsi
drop index ORDERS.O_C_ID using gsi
drop index ORDERS.O_D_ID using gsi
drop index ORDER_LINE.OL_W_I_ID using gsi
drop index STOCK.S_W_I_ID using gsi
drop index WAREHOUSE.WH_ID using gsi
drop primary index on CUSTOMER using gsi
drop primary index on DISTRICT using gsi
drop primary index on HISTORY using gsi
drop primary index on ITEM using gsi
drop primary index on NEW_ORDER using gsi
drop primary index on ORDERS using gsi
drop primary index on ORDER_LINE using gsi
drop primary index on STOCK using gsi
drop primary index on WAREHOUSE using gsi
create index CU_ID_D_ID_W_ID on CUSTOMER(C_ID, C_D_ID, C_W_ID) using gsi 
create index CU_W_ID_D_ID_LAST on CUSTOMER(C_W_ID, C_D_ID, C_LAST) using gsi 
create index DI_ID_W_ID on DISTRICT(D_ID, D_W_ID) using gsi 
create index IT_ID on ITEM(I_ID) using gsi 
create index NO_D_ID_W_ID on NEW_ORDER(NO_O_ID, NO_D_ID, NO_W_ID) using gsi 
create index OR_O_ID_D_ID_W_ID on ORDERS(O_ID, O_D_ID, O_W_ID, O_C_ID) using gsi 
create index OR_W_ID_D_ID_C_ID on ORDERS(O_W_ID, O_D_ID, O_C_ID) using gsi 
create index OL_O_ID_D_ID_W_ID on ORDER_LINE(OL_O_ID, OL_D_ID, OL_W_ID) using gsi 
create index ST_W_ID_I_ID1 on STOCK(S_W_ID, S_I_ID) using gsi 
create index WH_ID on WAREHOUSE(W_ID) using gsi 
create index D_ID on DISTRICT(D_ID)
create index C_ID on CUSTOMER(C_ID)
create index O_C_ID on ORDERS(O_C_ID)
create index O_D_ID on ORDERS(O_D_ID)
create index OL_W_I_ID on ORDER_LINE(OL_W_ID, OL_I_ID)
create index S_W_I_ID on STOCK(S_W_ID, S_I_ID)
select keyspace_id, state from system:indexes
select keyspace_id, state from system:indexes where state != 'online'
