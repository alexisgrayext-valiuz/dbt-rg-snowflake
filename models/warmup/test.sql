--CREATE OR REPLACE TABLE FD_IO_DLK_RG.DLK_RG_BDA_ACQUISITION.agg_ticket_rg AS (

WITH first_ticket as (
    SELECT master_customer_id,
           min(purchase_date) as date_first_ticket
    from FD_IO_DLK_RG.DLK_RG_BDA_ACQUISITION.tmp_agg_ticket_rg
    group by master_customer_id
)

SELECT a.master_customer_id, 
       
       min(purchase_date) as date_first_purchase,
       max(purchase_date) as date_last_purchase,
       AVG(DATEDIFF(DAY, lag_date_purchase, purchase_date)) as avg_delta_jour,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then  amount_including_taxes_web end ) AS amount_including_taxes_web_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then amount_including_taxes_mag end ) AS amount_including_taxes_store_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then  amount_including_taxes_day end ) AS amount_including_taxes_12m, 
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then nb_ticket_web end ) AS nb_ticket_web_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then  nb_ticket_mag end ) AS nb_ticket_store_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then  nb_ticket end ) AS nb_ticket_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then  nb_achat_actif end ) AS nb_achat_actif_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then  margin_excluding_taxes end ) as margin_excluding_taxes_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then margin_excluding_taxes_web end ) as margin_excluding_taxes_web_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then margin_excluding_taxes_mag end ) as margin_excluding_taxes_mag_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then qte_web end ) AS nb_product_web_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then qte_mag end ) AS nb_product_store_12m,
       SUM(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then qte end ) AS nb_product_12m,
       sum(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then nb_art end ) as nb_art_12m,
       sum(abs(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then nb_retour end )) as nb_retour_12m,
       sum(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then customer_markdown_including_taxes end ) as customer_markdown_including_taxes_12m,
       sum(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then customer_discount_including_taxes end ) as customer_discount_including_taxes_12m,
       sum(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then initial_selling_price end ) as initial_selling_price_12m,
       sum(case when purchase_date < DATEADD(MONTH, 12, b.date_first_ticket) then  Total_Discount end )  as Total_Discount_12M,

       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then amount_including_taxes_web else 0 end) AS amount_including_taxes_web_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then amount_including_taxes_mag else 0 end) AS amount_including_taxes_store_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then amount_including_taxes_day else 0 end) AS amount_including_taxes_0_6m, 
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then nb_ticket_web else 0 end) AS nb_ticket_web_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then nb_ticket_mag else 0 end) AS nb_ticket_store_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then nb_ticket else 0 end) AS nb_ticket_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then nb_achat_actif else 0 end) AS nb_achat_actif_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then margin_excluding_taxes else 0 end) as margin_excluding_taxes_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then margin_excluding_taxes_web else 0 end) as margin_excluding_taxes_web_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then margin_excluding_taxes_mag else 0 end) as margin_excluding_taxes_mag_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then qte_web else 0 end) AS nb_product_web_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then qte_mag else 0 end) AS nb_product_store_0_6m,
       SUM(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then qte else 0 end) AS nb_product_0_6m,
       sum(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then nb_art else 0 end) as nb_art_0_6m,
       sum(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then abs(nb_retour) else 0 end) as nb_retour_0_6m,
       sum(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then customer_markdown_including_taxes else 0 end) as customer_markdown_including_taxes_0_6m,
       sum(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then customer_discount_including_taxes else 0 end) as customer_discount_including_taxes_0_6m,
       sum(case when(purchase_date < DATEADD(MONTH, 6, b.date_first_ticket)) then initial_selling_price else 0 end) as initial_selling_price_0_6m,

       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then amount_including_taxes_web else 0 end) AS amount_including_taxes_web_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then amount_including_taxes_mag else 0 end) AS amount_including_taxes_store_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then amount_including_taxes_day else 0 end) AS amount_including_taxes_6_12m, 
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then nb_ticket_web else 0 end) AS nb_ticket_web_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then nb_ticket_mag else 0 end) AS nb_ticket_store_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then nb_ticket else 0 end) AS nb_ticket_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then nb_achat_actif else 0 end) AS nb_achat_actif_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then margin_excluding_taxes else 0 end) as margin_excluding_taxes_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then margin_excluding_taxes_web else 0 end) as margin_excluding_taxes_web_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then margin_excluding_taxes_mag else 0 end) as margin_excluding_taxes_mag_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then qte_web else 0 end) AS nb_product_web_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then qte_mag else 0 end) AS nb_product_store_6_12m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then qte else 0 end) AS nb_product_6_12m,
       sum(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then nb_art else 0 end) as nb_art_6_12m,
       sum(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then abs(nb_retour) else 0 end) as nb_retour_6_12m,
       sum(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then customer_markdown_including_taxes else 0 end) as customer_markdown_including_taxes_6_12m,
       sum(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then customer_discount_including_taxes else 0 end) as customer_discount_including_taxes_6_12m,
       sum(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then initial_selling_price else 0 end) as initial_selling_price_6_12m,
       sum(case when(purchase_date >= DATEADD(MONTH, 6, b.date_first_ticket) and purchase_date < DATEADD(YEAR, 1, b.date_first_ticket)) then Total_Discount else 0 end) as Total_Discount_6_12m,
       
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then amount_including_taxes_web else 0 end) AS amount_including_taxes_web_12m_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then amount_including_taxes_mag else 0 end) AS amount_including_taxes_store_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then amount_including_taxes_day else 0 end) AS amount_including_taxes_12_18m, 
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then nb_ticket_web else 0 end) AS nb_ticket_web_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then nb_ticket_mag else 0 end) AS nb_ticket_store_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then nb_ticket else 0 end) AS nb_ticket_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then nb_achat_actif else 0 end) AS nb_achat_actif_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then margin_excluding_taxes else 0 end) as margin_excluding_taxes_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then margin_excluding_taxes_web else 0 end) as margin_excluding_taxes_web_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then margin_excluding_taxes_mag else 0 end) as margin_excluding_taxes_mag_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then qte_web else 0 end) AS nb_product_web_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then qte_mag else 0 end) AS nb_product_store_12_18m,
       SUM(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then qte else 0 end) AS nb_product_12_18m,
       sum(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then nb_art else 0 end) as nb_art_12_18m,
       sum(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then abs(nb_retour) else 0 end) as nb_retour_12_18m,
       sum(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then customer_markdown_including_taxes else 0 end) as customer_markdown_including_taxes_12_18m,
       sum(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then customer_discount_including_taxes else 0 end) as customer_discount_including_taxes_12_18m,
       sum(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then initial_selling_price else 0 end) as initial_selling_price_12_18m,
       sum(case when(purchase_date >= DATEADD(MONTH, 12, b.date_first_ticket) and purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)) then Total_Discount else 0 end) as Total_Discount_12_18m,    

       SUM(case when(purchase_date = date_first_ticket) then amount_including_taxes_day else 0 end) AS amount_including_taxes_first_ticket, 
       SUM(case when(purchase_date = date_first_ticket) then margin_excluding_taxes else 0 end) as margin_excluding_taxes_first_ticket,
       SUM(case when(purchase_date = date_first_ticket) then qte else 0 end) AS nb_product_first_ticket,
       sum(case when(purchase_date = date_first_ticket) then nb_art else 0 end) as nb_art_first_ticket,
       sum(case when(purchase_date = date_first_ticket) then customer_markdown_including_taxes else 0 end) as customer_markdown_including_taxes_first_ticket,
       sum(case when(purchase_date = date_first_ticket) then customer_discount_including_taxes else 0 end) as customer_discount_including_taxes_first_ticket,
       sum(case when(purchase_date = date_first_ticket) then initial_selling_price else 0 end) as initial_selling_price_first_ticket,
       sum(case when(purchase_date = date_first_ticket) then Total_Discount else 0 end) as Total_Discount_first_ticket,
       sum(case when(purchase_date = date_first_ticket) then amount_ventes else 0 end) as amount_ventes_first_ticket
      
from FD_IO_DLK_RG.DLK_RG_BDA_ACQUISITION.tmp_agg_ticket_rg a
left join first_ticket b
on a.master_customer_id = b.master_customer_id

where purchase_date < DATEADD(MONTH, 18, b.date_first_ticket)
and purchase_date >= b.date_first_ticket
group by a.master_customer_id

--)