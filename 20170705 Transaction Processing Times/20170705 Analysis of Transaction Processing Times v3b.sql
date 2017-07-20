/*====================================================================================================================*/
/*==                                            Useful Functions                                                     ==*/
/*====================================================================================================================*/
/* Get the quarter from a date ( 2017Q1, 2017Q2 ... ) */
CREATE OR REPLACE FUNCTION get_quarter(timestamp)
RETURNS char(2)
LANGUAGE sql
AS
$$
    SELECT cast(extract(year from $1) as char(4))||'Q'||cast(floor((extract(month from $1)-1)/3)+1 as char(1)) as year_quarter
$$;


/* Get the date when the last complete quarter ended */
CREATE OR REPLACE FUNCTION get_last_quarter_end_date(TIMESTAMP WITH TIME ZONE)
RETURNS date
LANGUAGE sql
AS
$$
SELECT cast(CASE WHEN EXTRACT(MONTH FROM $1) > 9 THEN EXTRACT(YEAR FROM $1) || '0930'
            WHEN EXTRACT(MONTH FROM $1) > 6 THEN EXTRACT(YEAR FROM $1) || '0630'
            WHEN EXTRACT(MONTH FROM $1) > 3 THEN EXTRACT(YEAR FROM $1) || '0331'
            ELSE EXTRACT(YEAR FROM $1) - 1 || '0131'
            END AS DATE)

$$;


/*
Calculate difference in minutes

Note: Rounding is performed in the conventional intuitive manner (up or down to the closest integer)
*/
CREATE OR REPLACE FUNCTION minute_difference(timestamp, timestamp)
RETURNS int
LANGUAGE sql
AS
$$
    SELECT cast(EXTRACT(epoch FROM $1 - $2)/60 as int) as DateDifference
$$;

CREATE OR REPLACE FUNCTION day_difference(timestamp, timestamp)
RETURNS int
LANGUAGE sql
AS
$$
    SELECT cast(EXTRACT(epoch FROM $1 - $2)/60/60/24 as int) as DateDifference
$$;


/* Create minute bands convenient for transaction monitoring */
CREATE OR REPLACE FUNCTION minute_banding(int)
RETURNS VARCHAR(12)
LANGUAGE sql
AS
$$
    SELECT
     case when $1 < 10      then 'A. <10 min'
          when $1 < 60      then 'B. <60 min'
          when $1 < 3*60    then 'C. <3 hours'
          when $1 < 6*60    then 'D. <6 hours'
          when $1 < 12*60   then 'E. <12 hours'
          when $1 < 24*60   then 'F. <1 day'
          when $1 < 3*24*60 then 'G. <3 days'
          when $1 >=3*24*60 then 'H. 3+ days'
        else 'N/A'
      end
      as banding
$$;




grant execute on function get_quarter(timestamp)  to public;
grant execute  on function get_last_quarter_end_date(TIMESTAMP WITH TIME ZONE)  to public;
grant execute  on function minute_difference(timestamp, timestamp)  to public;
grant execute  on function day_difference(timestamp, timestamp)  to public;
grant execute  on function minute_banding(int)  to public;
grant execute  on function transaction_event_waterfall(int)  to public;

/*====================================================================================================================*/
/*==                                  Transaction Processing Time Analysis                                          ==*/
/*====================================================================================================================*/

/* Core transaction timing information */


--select count(*) from cd_data.transaction_lapsed_pre_post_auth

drop table if exists RQ20170705_PAID_TRANSACTIONS_ANALYSIS;

create table RQ20170705_PAID_TRANSACTIONS_ANALYSIS as


          SELECT
            a.transaction_id,
            a.user_fk,
            a.sender_country,
            a.receiver_country,
            d.receive_method_name,
            c.correspondent_name,
            a.payment_method_name,
            a.card_type,
            a.created_at,
            b.authorized_time,
            b.received_time,
            b.paid_time,
            a.is_first_transaction,

            (t.sent_amount/t.fx_rate_gbp_to_sent_amount) as amount_sent,
            t.revenue AS revenue,
            t.fx_gain as revenue_fx,
            (t.fees-t.voucher_cost)/t.fx_rate_gbp_to_sent_amount as revenue_fee,

            CASE WHEN B.last_pre_auth_event  IS NOT NULL THEN 1 ELSE 0 END as FL_PRE_AUTH,
            CASE WHEN B.last_post_auth_event IS NOT NULL THEN 1 ELSE 0 END as FL_POST_AUTH,

            --Pre Auth
            minute_difference(b.last_pre_auth_event,b.received_time) AS time_lapse_creation_to_preauth,
            minute_difference(b.authorized_time,b.last_pre_auth_event) AS time_lapse_preauth_to_authorized,
            minute_difference(b.authorized_time,b.received_time)  AS time_lapse_creation_to_authorized,

            --Post Auth
            minute_difference(b.last_post_auth_event,b.authorized_time) AS time_lapse_auth_to_postauth,
            minute_difference(b.paid_time,b.last_post_auth_event)  AS time_lapse_postauth_to_paid,
            minute_difference(b.paid_time,b.authorized_time)  AS time_lapse_authorized_to_paid,

            --Overall Total
            minute_difference(b.paid_time,b.received_time)  AS   time_lapse_creation_to_paid,

            --Paid or Cancelled
            case when paid_date is not null then 1 else 0 end as FL_paid,
            case when cancellation_date is not null then 1 else 0 end as FL_cancel

          FROM os_data.TRANSACTION a
            LEFT JOIN cd_data.transaction_lapsed_pre_post_auth b ON a.transaction_id = b.transaction_fk
            LEFT JOIN os_data.correspondent C ON a.correspondent_fk = C.correspondent_id
            LEFT JOIN os_data.receive_method D ON a.receive_method_fk = D.receive_method_id

          WHERE a.created_at BETWEEN '2016-01-01 00:00:00'  AND get_last_quarter_end_date(now());

create index on RQ20170705_PAID_TRANSACTIONS_ANALYSIS(user_fk);
create index on RQ20170705_PAID_TRANSACTIONS_ANALYSIS(transaction_id);

drop table test ;

create temporary table test as
select user_fk, count(distinct date_trunc('month', created_at))
from RQ20170705_PAID_TRANSACTIONS_ANALYSIS
group by user_fk;




alter table RQ20170705_PAID_TRANSACTIONS_ANALYSIS
  add amount_sent double precision,
  add revenue double precision,
  add revenue_fx double precision,
  add revenue_fee double precision;


update RQ20170705_PAID_TRANSACTIONS_ANALYSIS A
  SET amount_sent = b.sent_amount,
      revenue = b.Revenue,
      revenue_fx = b.fx_revenue,
      revenue_fee = b.fee_revenue
from tmp_reve b where a.transaction_id = b.transaction_id;



/*====================================================================================================================*/
/*==                                  Transaction Friction Causes Analysis                                          ==*/
/*====================================================================================================================*/


/* Auxiliary function: Create minute bands convenient for transaction monitoring */
CREATE OR REPLACE FUNCTION transaction_event_waterfall(int, varchar)
RETURNS VARCHAR(12)
LANGUAGE sql
AS
$$
SELECT CASE

       /* KYC */
       WHEN $1 = 10
         THEN 'A1) BLACK LIST'
       WHEN $1 = 27
         THEN 'A2) FIRST TRANSACTION HOLD'
       WHEN $1 = 29 and $2 <> 'Airtime Topup'
         THEN 'A3) PROVE ID FAULT'
       WHEN $1 = 37
         THEN 'A4) VERIFY SENDER AGE'
       WHEN $1 = 38
         THEN 'A5) FRAUD LIST'
       WHEN $1 = 39
         THEN 'A6) SUSPECT MATCH'

       /* Changes */
       WHEN $1 = 5
         THEN 'B1) BENEFICIARY CHANGE REQUEST'
       WHEN $1 = 42
         THEN 'B2) SENDER CHANGE REQUEST'
       WHEN $1 = 21
         THEN 'B3) EDITED / UNDO'
       WHEN $1 = 23
         THEN 'B3) EDITED / UNDO'
       WHEN $1 = 24
         THEN 'B3) EDITED / UNDO'

       /* Payment Provider Issue */
       WHEN $1 = 30
         THEN 'D1) DEFERRED'

       /* API Fails */
       WHEN $1 = 34
         THEN 'E1) API FAILED'

       /* Generic  Hold */
       WHEN $1 = 3
         THEN 'X1) HOLD'

       /* Apparently no issue, just clearing */
       WHEN $1 = 28
         THEN 'D2) CLEARING'

       END
  AS banding
$$;


/* Auxiliary function: Removes HTML tags and unique identifiers from comments */
CREATE OR REPLACE FUNCTION clean_comment(varchar)
RETURNS VARCHAR(150)
LANGUAGE sql
AS
$$

SELECT CASE
      WHEN $1 like '%AMOUNT:%MORE THAN%TRANSAC%' then 'World Remit TME: # Transactions'
      WHEN $1 like '%AMOUNT:%REVIEW%-%TO%' then 'World Remit TME: Corridor'
      WHEN $1 like '%AMOUNT:%' then 'World Remit TME: Others'
      WHEN $1 LIKE 'STARTTRANSACTION%SUCCESS%' THEN NULL
      WHEN $1 LIKE 'WORLDCOMPLIANCE%' THEN 'WorldCompliance List'
      ELSE REGEXP_REPLACE(
           REGEXP_REPLACE($1, '(<.*)', ' ', 'ig'),
           '(((REF NUMBER IS|REF|TXN ID|TRANSACTIONID|TRANSACTION|TRANSACTION REFERENCE|TRANSACTION PAID -|TRACENO|STATEMENTID|REFERENCE NUMBER|FILE|AMOUNT|ID|TXN|DATE|TRACE|CARD|SCORE)( ?= ?| ?: | ))(\S)*)',
           '', 'g') END
  AS clean_comment
$$;





/* Classification of the most granular issue for each transaction, pre and post */
-- Events are prioritized by their level of detail, not by date, i.e. we get the most informative status we can identify
--20m

DROP TABLE IF EXISTS TMP_transaction_events;

CREATE TEMPORARY TABLE TMP_transaction_events AS
  SELECT
    T.transaction_id,
    min(transaction_event_waterfall(status_fk,receive_method_name))         AS transaction_friction,
    min(CASE WHEN E.timestamp >= T.received_time AND E.timestamp <= T.authorized_time
      THEN transaction_event_waterfall(status_fk,receive_method_name) END)  AS transaction_friction_preauth,
    min(CASE WHEN E.timestamp > T.authorized_time AND E.timestamp <= T.paid_time
      THEN transaction_event_waterfall(status_fk,receive_method_name) END)  AS transaction_friction_postauth
  FROM os_data.transaction_event E
    INNER JOIN RQ20170705_PAID_TRANSACTIONS_ANALYSIS T ON t.transaction_id = e.transaction_fk
  WHERE TIMESTAMP BETWEEN '2016-01-01 00:00:00' AND get_last_quarter_end_date(now())
  GROUP BY T.transaction_id;




alter table RQ20170705_PAID_TRANSACTIONS_ANALYSIS
    add transaction_friction varchar(30),
    add transaction_friction_preauth varchar(30),
    add transaction_friction_postauth varchar(30),
    add transaction_friction_preauth_detail varchar(60),
    add transaction_friction_postaut_detail varchar(60);


UPDATE RQ20170705_PAID_TRANSACTIONS_ANALYSIS a
SET transaction_friction          = null
  , transaction_friction_preauth  = null
  , transaction_friction_postauth = null;


UPDATE RQ20170705_PAID_TRANSACTIONS_ANALYSIS a
SET transaction_friction          = b.transaction_friction
  , transaction_friction_preauth  = b.transaction_friction_preauth
  , transaction_friction_postauth = b.transaction_friction_postauth
FROM TMP_transaction_events B
WHERE a.transaction_id = b.transaction_id;

/*
select transaction_id from RQ20170705_PAID_TRANSACTIONS_ANALYSIS where transaction_friction_preauth = 'D2) CLEARING' limit 100
*/


/*======================================          Comment-Level details         ======================================*/


/* Auxiliary function: Reclassify Hold comments in a more intelligible way*/
CREATE OR REPLACE FUNCTION classify_hold_comments(varchar)
RETURNS VARCHAR(60)
LANGUAGE sql
AS
$$
SELECT
  CASE


    WHEN $1 like '%AMOUNT:%MORE THAN%TRANSAC%' or $1 like 'World Remit TME: # Transactions'  then 'World Remit TME: # Transactions'
    WHEN $1 like '%AMOUNT:%REVIEW%-%TO%' or $1 like 'World Remit TME: Corridor' then 'World Remit TME: Corridor'
    WHEN $1 like '%AMOUNT:%' or $1 like 'World Remit TME: Others' then 'World Remit TME: Others'

    WHEN $1 = 'ZORAL FRAUD RESULT COLOR IS: AMBER' THEN 'Fraud: Zoral'
    WHEN $1 = 'WorldCompliance List' THEN 'Fraud: World Compliance List'
    WHEN $1 LIKE 'BRIDGER%' THEN 'Fraud: Bridger Sanction List'
    WHEN $1 = ' REASON: X201 : RECEIVED ERROR FROM DOWNSTREAM ' THEN 'Server Error'
    WHEN $1 =
         ' REASON: A125 : CANNOT PROCESS TWO CONCURRENT SECONDARY TRANSACTIONS THAT REFER THE SAME PRIMARY TRANSACTION' THEN 'Already Processing'
    WHEN $1 = 'PAYPOINT CAPTURE FAILED: CONNECTION ERROR' THEN 'Server Error'
    WHEN $1 = ' REASON: D100 : DECLINED BY UPSTREAM PROCESSOR' THEN 'Server Error'
    WHEN $1 =
         'THIS ACCOUNT HAS BEEN ADDED TO A RISK ALERT LIST - REFER TO FRAUD BEFORE RELEASING.' THEN 'Fraud: Others'
    WHEN $1 = ' REASON: V100 : PREAUTH  IS VOID.' THEN 'Fraud: Others'
    WHEN $1 = 'CORRIDOR HAS CHANGED AND ACCOUNT HAS BEEN INACTIVE FOR OVER 30 DAYS. [SENDER ' THEN 'Other'
    WHEN $1 = 'ZORAL FRAUD RESULT COLOR IS: RED' THEN 'Fraud: Zoral'
    WHEN $1 = ' REASON: V100 :  ALREADY BEING PROCESSED' THEN 'Already Processing'
    WHEN $1 = ' REASON: V105 : RELATED  FOUND FOR THIS MERCHANT' THEN 'Already Processing'
    WHEN $1 = ' REASON: X201 : FAILED TO COMMUNICATE WITH DESTINATION ' THEN 'Server Error'
    WHEN $1 = ' REASON: V139 : CANNOT PERFORM A CAPTURE AGAINST AN ALREADY-VO' THEN 'Already Processing'
    WHEN $1 = ' REASON: E500 : INTERNAL SERVER ERROR' THEN 'Server Error'
    WHEN $1 = ' REASON: V136 : CANNOT PERFORM A CAPTURE AGAINST A FAILED TRANSACTION' THEN 'Other'
    WHEN $1 = 'NOT FOUND' THEN 'Other'
    WHEN $1 = ' REASON: V100 : PREAUTH  HAS ALREADY BEEN CAPTURED.' THEN 'Already Processing'
    WHEN $1 = 'INTERNAL SERVER ERROR' THEN 'Server Error'
    WHEN $1 = 'REQUEST FAILED' THEN 'Other'
    WHEN $1 = ' REASON: A104 : UNABLE TO DETERMINE  FOR ' THEN 'Other'
    WHEN $1 = ' REASON: V134 : CANNOT PERFORM A CAPTURE AGAINST A PAYMENT TRANSACTION' THEN 'Other'
    WHEN $1 = 'SERVICE UNAVAILABLE' THEN 'Server Error'
    ELSE 'Other'
    END
  AS banding
$$;

/* Auxiliary function: Reclassify API comments in a more intelligible way*/
CREATE OR REPLACE FUNCTION classify_apifail_comments(varchar)
RETURNS VARCHAR(60)
LANGUAGE sql
AS
$$
 SELECT
CASE
  WHEN $1 LIKE '%FTP%' THEN 'FTP Issue'
  WHEN $1 LIKE '%MOBILE NUMBER VALIDATION%' OR
       $1 LIKE '%MOBILE%VALIDATION%' THEN 'Mobile Number Issue'
  WHEN $1 LIKE '%CUSTOMER%REQUEST%' OR
       $1 LIKE '%RETURNED: AS PER%MAIL%' THEN 'Customer/Mail Request'
  WHEN $1 LIKE '%KYC%' THEN 'KYC Failed'
  WHEN $1 LIKE '%DUPLICATE%' THEN 'Duplicate'
  WHEN
    $1 LIKE '%SECURE%SSL%' OR $1 LIKE '%TCP ERROR%' OR $1 LIKE '%MAX WAIT TIME%'
    OR $1 LIKE '%MAX NUMBER OF ATTEMPTS WAS REACHED%' OR $1 LIKE '%TOO MANY CONNECTION%' OR
    $1 LIKE '%DESTINATION SYSTEM ROUTE ERROR%' OR $1 LIKE '%JDBC%CONNECTION%' OR
    $1 LIKE '%TIMEOUT%' OR $1 LIKE '%UNABLE%TO%CONNECT%' OR
    $1 LIKE '%CONNECTIONERROR%' OR $1 LIKE '%BINDING%' OR
    $1 LIKE '%MAXIMUM NUMBER OF ATTEMPTS%' OR $1 LIKE '%NO ENDPOINT%' OR
    $1 LIKE '%CONNECTION REFUSED%' THEN 'Timeout / Server Offline'
  WHEN $1 LIKE '%SUBSCRIBER%NOT FOUND%' OR $1 LIKE '%ACCOUNT%NOT FOUND%' OR
       $1 LIKE '%ACCOUNT%NOT VALID%' OR $1 LIKE '%PAYER_NOT_FOUND%' OR
       $1 LIKE '%UNKNOWN RECEIVER%' OR $1 LIKE '%UNABLETOCREDITDESTINATIONACCOUNT%' OR
       $1 LIKE '%UNABLE TO FIND A RECEIVER%' OR $1 LIKE '%NO ACCOUNT%' OR
       $1 LIKE '%ACCOUNT%RANGE ERROR%' OR $1 LIKE '%BARRED%' OR
       $1 LIKE '%INCORRECT%' OR $1 LIKE '%INVAL%' OR $1 LIKE '%blocked%' OR
       $1 LIKE '%FROZEN%' OR UPPER($1) LIKE '%CLOSED%' OR
       UPPER($1) LIKE '%PAYEE_NOT_FOUND%' THEN 'Invalid or Closed Account'

  WHEN $1 LIKE 'AOC SUCCEED, BUT REMITTANCE FAILED. MAXIMUM%' OR $1 LIKE '%NO FUNDS%' OR
       $1 LIKE '%LIMIT%' OR $1 LIKE '%INSUFFI%' OR $1 LIKE '%LIMIT EXCEEDED%' OR
       $1 LIKE '%MINIMUMAMOUNT%' OR $1 LIKE '%BELOW MINIMUM%' OR
       $1 LIKE '%START FOR API  AND RETURNED AOC SUCCEED%MAXIMUM%' OR
       $1 LIKE '%MIN%CASH%' OR $1 LIKE '%MAX%AMOUNT%' OR
       $1 LIKE '%MAX%TOPUP%' THEN 'Insufficient Funds / Limit Exceeded (MAX/MIN)'

  WHEN
    $1 LIKE '%CODE%REQUIRED%' OR $1 LIKE '%missing%' OR $1 LIKE '%NO.%REQUIRED%'
    OR $1 LIKE '%BENEF%REQUIRED%' OR $1 LIKE '%ACCOUNT%REQUIRED%' OR
    $1 LIKE '%NAME%REQUIRED%' OR $1 LIKE '%TAXPAYER%REQUIRED%' OR
    $1 LIKE '%BRANCH%REQUIRED%' OR $1 LIKE '%RECIPIENT%REQUIRED%' OR
    $1 LIKE '%MISSING%ADDRESS%' OR $1 LIKE '%MISSING%IDEN%' OR
    $1 LIKE '%MISSING%SENDER%' THEN 'More Info Required'
  WHEN $1 LIKE '%PIN%REQUIRED%' OR $1 LIKE '%PIN%tries%' THEN 'PIN Required'



  WHEN $1 LIKE '%GETSTATUS%FAILED (%)%' OR
       $1 LIKE 'CHECKSTATUS%DATA NOT FOUND%' OR
       $1 LIKE '%GETTRANSACTIONSTATUS RETURNED%NOT SUPPORTED%' OR
       $1 LIKE '%GETTRANSACTIONSTATUS. RESPONSECODE%' THEN 'Failed to get transaction status'

  WHEN $1 LIKE '%TOO SHORT%' OR $1 LIKE '%TOO LONG%' OR $1 LIKE '%PATTERN%' OR
       $1 LIKE '%OUTSIDE THE BOUNDS OF THE ARRAY.%' OR $1 LIKE '%LENGTH%' OR
       $1 LIKE '%CHARACTER%' OR $1 LIKE '%MAX%LENGTH%' THEN 'Data format issue'

  WHEN $1 LIKE '%DOMESTIC%' OR $1 LIKE '%DONATION%' OR $1 LIKE '%INSTITUTION%' OR
       $1 LIKE '%SOCIETY%' OR $1 LIKE '%ASSOCIATION%' OR $1 LIKE '%CHARITY%' OR
       $1 LIKE '%FOUNDATION%' OR $1 LIKE '%FUND COMPANY%' THEN 'Transfer to Society / Institution'

  WHEN $1 LIKE '%REJECTED BY%BANK%' OR $1 LIKE '%RETURNED BY BANK%' OR
       $1 LIKE '%AS%PER%EXCHANGE%HOUSE%REJ%' OR $1 LIKE '%RETURNED%AS PER%BANK%' OR
       $1 LIKE '%EXCHANGE HOUSE REJECTED%' THEN 'Rejected/Returned by Bank'
  WHEN $1 LIKE '%CANCELLATION REQUESTED BY WORLDREMIT/SENDER%' THEN 'Cancelled by WR'


  --Lower priority, capturing some very generic stuff
  WHEN $1 LIKE '%SERVER%ERROR%' THEN 'Timeout / Server Offline'
  WHEN $1 LIKE '%RETURNED%ACCOUNT%NOT%' THEN 'Invalid or Closed Account'

  WHEN $1 LIKE '%START FOR API  AND RETURNED THE CODE: 10001%' OR $1 LIKE '%(ERRORCODE%' OR
       $1 LIKE '%VO WREMIT%'
       OR $1 LIKE '%GETTRANSACTIONSTATUS RETURNED: 1 - COULD NOT DETERMINE%'
       OR $1 LIKE '%UNKNOWN%ERROR%'
       OR $1 LIKE '%UNEXPECTED%ERROR%'
       OR $1 LIKE '%EXECUTION%RESET%'
       OR $1 LIKE '%UNABLE TO PROCESS%'
       OR $1 LIKE '%OBJECT REFERENCE NOT SET TO AN INSTANCE OF AN OBJECT%'
       OR $1 LIKE '%START 28-%'
       OR $1 LIKE '%CANCELLED BY API%'
       OR $1 LIKE 'START FOR API%SYSTEM MALFUNCTION%'
       OR $1 LIKE 'START FOR API%UNKNOWN RESPONSE%'
       OR $1 LIKE '%MAX NUMBER OF ATTEMPTS EXCEED%'
       OR $1 LIKE '%API FAILED%'
       OR $1 LIKE '%START FOR API%AND RETURNED%'

    THEN 'Generic API Error'

  WHEN $1 LIKE '%REVERSED%' OR $1 LIKE '%RETURNED%' OR
       $1 LIKE '%REJECTED%' THEN 'Other Reject/Return/Reverse'

  ELSE 'Others'   /*REGEXP_REPLACE($1,'((#|CODE:|REF:)(\S)*)','', 'g')*/
END
$$;




CREATE OR REPLACE FUNCTION dice(high INT)
   RETURNS INT
  language sql
  AS
  $$

   select cast(ceil(random()* high )   as int)
$$;




/* Auxiliary function: Reclassify Transfer Cancel comments in a more intelligible way*/
CREATE OR REPLACE FUNCTION classify_canceltransfer_comments(varchar)
RETURNS VARCHAR(12)
LANGUAGE sql
AS
$$
 SELECT
 CASE
    WHEN $1 NOT LIKE '%CANCEL%' then null
    WHEN $1 LIKE '%NAME%' OR $1 LIKE '%REGISTERED UNDER%' THEN 'Cancel Transmit: Name Issue'
    WHEN $1 LIKE '%NUMBER%' OR $1 LIKE '%NOT REGISTERED%' OR
         $1 LIKE '%NON REGISTERED%' THEN 'Cancel Transmit: Number Issue'
    WHEN $1 LIKE '%ACC%' OR $1 LIKE '%A/C%' THEN 'Cancel Transmit: Account Issue'
    WHEN $1 LIKE '%VALUE%' OR $1 LIKE '%AMOUNT%' OR
         $1 LIKE '%MTN%' THEN 'Cancel Transmit: Amount Issue'
    WHEN $1 LIKE '%TOMORROW%' OR $1 LIKE '%TO BE%' OR $1 LIKE '%TODAY%' OR $1 LIKE '%ON MON%' OR
         $1 LIKE '%RE%TRANS%' THEN 'Cancel Transmit: Delayed / Retransmitted'
    WHEN $1 LIKE '%BANK%' THEN 'Cancel Transmit: Bank Issue'
    ELSE 'Cancel Transmit: Others' END

$$;




/* Detail of errors for transactions on hold */
drop table if exists TMP_hold_comments;
create TEMPORARY  table TMP_hold_comments as
SELECT
  transaction_fk,
  timestamp as event_dt,
  clean_comment(comments) AS TRIMMED_COMMENT
FROM os_data.transaction_event
WHERE status_fk = 3;

drop table if exists TMP_hold_comments_clean;
CREATE TEMPORARY TABLE TMP_hold_comments_clean AS
  SELECT
    *,
    classify_hold_comments(TRIMMED_COMMENT) AS trimmed_comment_clean,
    row_number()
    OVER (
      PARTITION BY transaction_fk
      ORDER BY event_dt DESC ) AS rank
  FROM TMP_hold_comments
  where   TRIMMED_COMMENT is not null
;


UPDATE RQ20170705_PAID_TRANSACTIONS_ANALYSIS a
SET transaction_friction_preauth_detail = null;


UPDATE RQ20170705_PAID_TRANSACTIONS_ANALYSIS a
SET transaction_friction_preauth_detail = trimmed_comment_clean
FROM TMP_hold_comments_clean B
WHERE a.transaction_id = b.transaction_fk
      AND a.transaction_friction_preauth LIKE '%HOLD%'
      AND rank=1
      ;

select transaction_friction_preauth_detail, count(*)
  from RQ20170705_PAID_TRANSACTIONS_ANALYSIS
    group by transaction_friction_preauth_detail;


--select transaction_friction_preauth_detail, count(*) from RQ20170705_PAID_TRANSACTIONS_ANALYSIS where transaction_friction_preauth LIKE '%HOLD%' group by transaction_friction_preauth_detail






/* Detail of errors for transactions with API failures */
--Retrieve related causes of error (not trivial, as the error is shown in previous events, not in the fail event itself)
drop table if exists TMP_API_fail_duped;
create TEMPORARY  table TMP_API_fail_duped as

  --Transmission issues
  SELECT
    transaction_fk,
    timestamp                          AS event_dt,
    status_fk,
    classify_canceltransfer_comments(comments) AS TRIMMED_COMMENT
  FROM os_data.transaction_event
  WHERE status_fk = 1
        AND comments LIKE '%CANCEL%'

UNION ALL

  --API Issues
  SELECT
    transaction_fk,
    timestamp                             AS event_dt,
    status_fk,
    lag(clean_comment(comments))
    OVER (
      PARTITION BY transaction_fk
      ORDER BY timestamp, status_fk ASC ) AS TRIMMED_COMMENT
  FROM os_data.transaction_event a
  WHERE status_fk IN (33, 34)
        AND exists(SELECT 1
                   FROM os_data.transaction_event b
                   WHERE a.transaction_fk = b.transaction_fk AND b.status_fk = 34);


drop table if exists TMP_API_fail_comments;

create TEMPORARY  table TMP_API_fail_comments as
  select * from (
                  SELECT
                    transaction_fk,
                    event_dt,
                    TRIMMED_COMMENT,
                    status_fk,
                    rank()
                    OVER (
                      PARTITION BY transaction_fk
                      ORDER BY event_dt DESC ) as rank
                  FROM TMP_API_fail_duped
                  WHERE (TRIMMED_COMMENT IS NOT NULL AND TRIMMED_COMMENT <> '#')
                    AND status_fk in (1,34)
                ) duped
  where rank = 1;

create TEMPORARY  table TMP_API_fail_comments_clean as
SELECT
  transaction_fk,
  case when status_fk = 34 then classify_apifail_comments(TRIMMED_COMMENT) else TRIMMED_COMMENT end AS summarized_comments
FROM TMP_API_fail_comments;


UPDATE RQ20170705_PAID_TRANSACTIONS_ANALYSIS a
SET transaction_friction_postaut_detail = b.summarized_comments
FROM TMP_API_fail_comments_clean B
WHERE a.transaction_id = b.transaction_fk
      AND a.transaction_friction_postauth LIKE '%API FAILED%';

--select transaction_friction_postaut_detail, count(*) from RQ20170705_PAID_TRANSACTIONS_ANALYSIS where transaction_friction_postauth='D3) API FAILED' group by transaction_friction_postaut_detail


/*====================================================================================================================*/
/*==                                    Repeat Sales and Inactivity                                           ==*/
/*====================================================================================================================*/
--Duration: 36m to 1h

DROP TABLE IF EXISTS TMP_transactions;

CREATE TEMPORARY TABLE TMP_transactions AS
  SELECT
    A.transaction_id,
    sum(
        CASE WHEN B.created_at BETWEEN A.created_at + INTERVAL '1 day' AND A.created_at + INTERVAL '3 month' THEN 1 ELSE 0 END)
      AS NUM_tansactions_3M,
    count(DISTINCT
        CASE WHEN B.created_at BETWEEN date_trunc('month', A.created_at) + INTERVAL '1 month' AND date_trunc('month', A.created_at) + INTERVAL '4 month' - INTERVAL '1 day'
          THEN EXTRACT(MONTH FROM B.created_at) END)
      AS NUM_months_with_transactions_3M
  FROM RQ20170705_PAID_TRANSACTIONS_ANALYSIS A
    LEFT JOIN RQ20170705_PAID_TRANSACTIONS_ANALYSIS B ON a.user_fk = b.user_fk
  WHERE a.created_at <= get_last_quarter_end_date(get_last_quarter_end_date(now()))
  GROUP BY A.transaction_id;
--We filter by end of the quarter before the previous, to remove info that is not mature enough.
--e.g. for the latest quarter we cannot calculate quarterly churn or repeat sale, so we only want to give the information from the quarter before the previous and older.

create index on TMP_transactions(transaction_id);

ALTER TABLE RQ20170705_PAID_TRANSACTIONS_ANALYSIS
  ADD NUM_tansactions_3M INTEGER,
  ADD NUM_months_with_transactions_3M INTEGER;


UPDATE RQ20170705_PAID_TRANSACTIONS_ANALYSIS A
SET NUM_tansactions_3M              = b.NUM_tansactions_3M
  , NUM_months_with_transactions_3M = b.NUM_months_with_transactions_3M
FROM TMP_transactions B
WHERE a.transaction_id = b.transaction_id;




/*====================================================================================================================*/
/*==                                    Customer Service Impact                                           ==*/
/*====================================================================================================================*/
--Duration: 36m to 1h

DROP TABLE IF EXISTS TMP_transactions;

CREATE TEMPORARY TABLE TMP_transactions AS
  SELECT
    A.transaction_id,
    count(*)  as num_cases
  FROM RQ20170705_PAID_TRANSACTIONS_ANALYSIS A
    LEFT JOIN os_data.sf_case_all B ON a.user_fk = b.user_fk and B.createddate >= A.created_at

  GROUP BY A.transaction_id;
--We filter by end of the quarter before the previous, to remove info that is not mature enough.
--e.g. for the latest quarter we cannot calculate quarterly churn or repeat sale, so we only want to give the information from the quarter before the previous and older.

create index on TMP_transactions(transaction_id);

ALTER TABLE RQ20170705_PAID_TRANSACTIONS_ANALYSIS
  ADD num_cases INTEGER;


UPDATE RQ20170705_PAID_TRANSACTIONS_ANALYSIS A
SET num_cases              = b.num_cases
FROM TMP_transactions B
WHERE a.transaction_id = b.transaction_id;










/*====================================================================================================================*/
/*==                                    Summarized Information for Output                                           ==*/
/*====================================================================================================================*/

/* Summarize information */
drop table if exists RQ2010705_TRANSACTION_TIME_ANALYSIS_PIVOT;

create table RQ2010705_TRANSACTION_TIME_ANALYSIS_PIVOT as

SELECT
  sender_country,
  receiver_country,
  receive_method_name,
  correspondent_name,
  payment_method_name, /*card_type,*/

  --Airtime topup triggers ID failures that are not true pre auth events (they are ignored in any practical sense)
  case when receive_method_name = 'Airtime Topup' and transaction_friction_preauth is null then 0 else FL_PRE_AUTH end as FL_PRE_AUTH,
  FL_POST_AUTH,
  case when is_first_transaction =1 then 'Y' else 'N' end as FL_first_transaction,
  case when FL_paid=1 then 'Y' else 'N' end as FL_paid,
  case when FL_cancel=1 then 'Y' else 'N' end as FL_cancelled,

  get_quarter(created_at)                           AS creation_quarter,

  minute_banding(time_lapse_creation_to_preauth)    AS band_lapse_creation_to_preauth,
  minute_banding(time_lapse_preauth_to_authorized)  AS band_lapse_preauth_to_authorized,
  minute_banding(time_lapse_creation_to_authorized) AS band_lapse_creation_to_authorized,
  minute_banding(time_lapse_auth_to_postauth)       AS band_lapse_auth_to_postauth,
  minute_banding(time_lapse_postauth_to_paid)       AS band_lapse_postauth_to_paid,
  minute_banding(time_lapse_authorized_to_paid)     AS band_lapse_authorized_to_paid,
  minute_banding(time_lapse_creation_to_paid)       AS band_lapse_creation_to_paid,

  transaction_friction,
  transaction_friction_preauth,
  transaction_friction_postauth,
  transaction_friction_preauth_detail,
  transaction_friction_postaut_detail,

  count(*)                                          AS transactions,
  sum(FL_PRE_AUTH)                                  AS transactions_preauth,
  sum(FL_POST_AUTH)                                 AS transactions_postauth,
  sum(FL_paid)                                      AS transactions_paid,
  sum(FL_cancel)                                    AS transactions_cancelled,
  sum(time_lapse_creation_to_preauth)               AS time_lapse_creation_to_preauth,
  sum(time_lapse_preauth_to_authorized)             AS time_lapse_preauth_to_authorized,
  sum(time_lapse_creation_to_authorized)            AS time_lapse_creation_to_authorized,
  sum(time_lapse_auth_to_postauth)                  AS time_lapse_auth_to_postauth,
  sum(time_lapse_postauth_to_paid)                  AS time_lapse_postauth_to_paid,
  sum(time_lapse_authorized_to_paid)                AS time_lapse_authorized_to_paid,
  sum(time_lapse_creation_to_paid)                  AS time_lapse_creation_to_paid,


  sum(case when a.NUM_tansactions_3M>0 then 1 else 0 end) as FL_tansactions_3M,
  sum(a.NUM_tansactions_3M) as NUM_tansactions_3M,
  sum(NUM_months_with_transactions_3M) as NUM_months_with_transactions_3M,

  sum(amount_sent) as amount_sent,
  sum(revenue) as revenue,
  sum(revenue_fx) as revenue_fx,
  sum(revenue_fee) as revenue_fee

FROM RQ20170705_PAID_TRANSACTIONS_ANALYSIS a
GROUP BY sender_country
  , receiver_country
  , receive_method_name
  , correspondent_name
  , payment_method_name /*, card_type,*/
  , FL_PRE_AUTH
  , FL_POST_AUTH
  , FL_paid
  , FL_cancel
  , FL_first_transaction
  , creation_quarter
  , band_lapse_creation_to_preauth
  , band_lapse_preauth_to_authorized
  , band_lapse_creation_to_authorized
  , band_lapse_auth_to_postauth
  , band_lapse_postauth_to_paid
  , band_lapse_authorized_to_paid
  , band_lapse_creation_to_paid
  , transaction_friction
  , transaction_friction_preauth
  , transaction_friction_postauth
  , transaction_friction_preauth_detail
  , transaction_friction_postaut_detail;