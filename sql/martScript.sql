INSERT INTO STV2023060622__DWH.global_metrics_copy (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
SELECT 
    transaction_dt::date as date_update, 
    t.currency_code as currency_from, 
    sum(amount * coalesce(currency_with_div, 1)) as amount_total, 
    COUNT(distinct operation_id) as cnt_transactions, 
    COUNT(distinct case when right(t.transaction_type, 8) = 'outgoing' then t.account_number_from else t.account_number_to end) AS avg_transactions_per_account, 
    COUNT(distinct case when right(t.transaction_type, 8) = 'outgoing' then t.account_number_from else t.account_number_to end) AS cnt_accounts_make_transactions
FROM STV2023060622__STAGING.transactions_copy t 
LEFT JOIN (
    SELECT 
        distinct currency_code, 
        first_value(currency_with_div) over (partition by currency_code order by date_update desc) AS currency_with_div
    from STV2023060622__STAGING.currencies_copy 
    where currency_code_with = 420
) c ON c.currency_code=t.currency_code 
WHERE 
    status = 'done' AND 
    transaction_dt::date = :date_param AND 
    t.account_number_from <> -1  AND 
    t.account_number_to <> -1 
GROUP BY 
    transaction_dt::date, t.currency_code;
