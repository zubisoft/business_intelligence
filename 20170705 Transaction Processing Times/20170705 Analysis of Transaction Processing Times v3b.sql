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


/* Create minute bands convenient for transaction monitoring */
CREATE OR REPLACE FUNCTION minute_banding(int)
RETURNS VARCHAR(12)
LANGUAGE sql
AS
$$
    SELECT
      case when $1 < 5      then 'A. <5 min'
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
grant execute  on function minute_banding(int)  to public;
grant execute  on function transaction_event_waterfall(int)  to public;

/*====================================================================================================================*/
/*==                                  Transaction Processing Time Analysis                                          ==*/
/*====================================================================================================================*/

/* Core transaction timing information */


--select count(*) from cd_data.transaction_lapsed_pre_post_auth

drop table if exists TIME_LAPSED_AUTH;

create table TIME_LAPSED_AUTH as

SELECT *
      ,ntile(4) OVER (  PARTITION BY receive_method_name      ORDER BY time_lapse_creation_to_authorized    ) AS quartile_time_lapse_creation_to_authorized
      ,ntile(4) OVER (  PARTITION BY receive_method_name      ORDER BY time_lapse_authorized_to_paid        ) AS quartile_time_lapse_authorized_to_paid
      ,ntile(4) OVER (  PARTITION BY receive_method_name      ORDER BY time_lapse_creation_to_paid          ) AS quartile_time_lapse_creation_to_paid
FROM (
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
            minute_difference(b.paid_time,b.received_time)  AS   time_lapse_creation_to_paid

          FROM os_data.TRANSACTION a
            LEFT JOIN cd_data.transaction_lapsed_pre_post_auth b ON a.transaction_id = b.transaction_fk
            LEFT JOIN os_data.correspondent C ON a.correspondent_fk = C.correspondent_id
            LEFT JOIN os_data.receive_method D ON a.receive_method_fk = D.receive_method_id

          WHERE a.created_at BETWEEN '2016-01-01 00:00:00'  AND get_last_quarter_end_date(now())
                AND a.paid_date IS NOT NULL

)  Y;

create index on TIME_LAPSED_AUTH(user_fk);
create index on TIME_LAPSED_AUTH(transaction_id);









/*====================================================================================================================*/
/*==                                  Transaction Friction Causes Analysis                                          ==*/
/*====================================================================================================================*/


/* Auxiliary function: Create minute bands convenient for transaction monitoring */
CREATE OR REPLACE FUNCTION transaction_event_waterfall(int)
RETURNS VARCHAR(12)
LANGUAGE sql
AS
$$
SELECT CASE
       WHEN $1 = 10
         THEN 'A1) BLACK LIST'
       WHEN $1 = 27
         THEN 'A2) FIRST TRANSACTION HOLD'
       WHEN $1 = 29
         THEN 'A3) PROVE ID FAULT'
       WHEN $1 = 37
         THEN 'A4) VERIFY SENDER AGE'
       WHEN $1 = 38
         THEN 'A5) FRAUD LIST'
       WHEN $1 = 39
         THEN 'A6) SUSPECT MATCH'
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
       WHEN $1 = 15
         THEN 'D1) RE-TRANSMITTED'
       WHEN $1 = 30
         THEN 'D2) CLEARING' --Explanation?
       WHEN $1 = 30
         THEN 'D3) DEFERRED'
       WHEN $1 = 34
         THEN 'D4) API FAILED'
       WHEN $1 = 35
         THEN 'D5) RE-SENT TO API'
       WHEN $1 = 3
         THEN 'X1) HOLD'
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
       WHEN $1 LIKE 'STARTTRANSACTION%SUCCESS%' THEN NULL
       WHEN $1 LIKE '%AMOUNT:%' THEN NULL
       WHEN $1 LIKE 'WORLDCOMPLIANCE%' THEN 'WorldCompliance List'
       ELSE REGEXP_REPLACE(
           REGEXP_REPLACE($1, '(<.*)', ' ', 'ig'),
           '(((REF NUMBER IS|REF|TXN ID|TRANSACTIONID|TRANSACTION|TRANSACTION REFERENCE|TRANSACTION PAID -|TRACENO|STATEMENTID|REFERENCE NUMBER|FILE|AMOUNT|ID|TXN|DATE|TRACE|CARD|SCORE)( ?= ?| ?: | ))(\S)*)',
           '', 'g') END
  AS clean_comment
$$;


/* Classification of the most granular issue for each transaction, pre and post */
DROP TABLE IF EXISTS TMP_transaction_events;

CREATE TEMPORARY TABLE TMP_transaction_events AS
  SELECT
    T.transaction_id,
    min(transaction_event_waterfall(status_fk))         AS transaction_friction,
    min(CASE WHEN E.timestamp >= T.received_time AND E.timestamp <= T.authorized_time
      THEN transaction_event_waterfall(status_fk) END)  AS transaction_friction_preauth,
    min(CASE WHEN E.timestamp > T.authorized_time AND E.timestamp <= T.paid_time
      THEN transaction_event_waterfall(status_fk) END)  AS transaction_friction_postauth
  FROM os_data.transaction_event E
    INNER JOIN TIME_LAPSED_AUTH T ON t.transaction_id = e.transaction_fk
  WHERE TIMESTAMP BETWEEN '2016-01-01 00:00:00' AND get_last_quarter_end_date(now())
  GROUP BY T.transaction_id;

create index on TMP_transaction_events(transaction_id);

alter table TIME_LAPSED_AUTH
    add transaction_friction varchar(30),
    add transaction_friction_preauth varchar(30),
    add transaction_friction_postauth varchar(30),
    add transaction_friction_preauth_detail varchar(30),
    add transaction_friction_postaut_detail varchar(30);

UPDATE TIME_LAPSED_AUTH a
SET transaction_friction          = b.transaction_friction
  , transaction_friction_preauth  = b.transaction_friction_preauth
  , transaction_friction_postauth = b.transaction_friction_postauth
FROM TMP_transaction_events B
WHERE a.transaction_id = b.transaction_id;




/* Detail of errors for transactions on hold */
create TEMPORARY  table TMP_hold_comments as
SELECT
  transaction_fk,
  clean_comment(comments) AS TRIMMED_COMMENT
FROM os_data.transaction_event
WHERE status_fk = 3;

UPDATE TIME_LAPSED_AUTH a
SET transaction_friction_preauth_detail = b.TRIMMED_COMMENT
FROM TMP_hold_comments B
WHERE a.transaction_id = b.transaction_id
      AND a.transaction_friction_preauth LIKE '%HOLD%';


/* Detail of errors for transactions with API failures */

--Retrieve related causes of error (not trivial, as the error is shown in previous events, not in the fail event itself)
create TEMPORARY  table TMP_API_fail_duped as

--Transmission issues
  SELECT
    transaction_fk,
    timestamp                          AS event_dt,
    status_fk,
    CASE WHEN comments LIKE '%NAME%' OR comments LIKE '%REGISTERED UNDER%' THEN 'Cancel Transmit: Name Issue'
    WHEN comments LIKE '%NUMBER%' OR comments LIKE '%NOT REGISTERED%' OR
         comments LIKE '%NON REGISTERED%' THEN 'Cancel Transmit: Number Issue'
    WHEN comments LIKE '%ACC%' OR comments LIKE '%A/C%' THEN 'Cancel Transmit: Account Issue'
    WHEN comments LIKE '%VALUE%' OR comments LIKE '%AMOUNT%' OR
         comments LIKE '%MTN%' THEN 'Cancel Transmit: Amount Issue'
    WHEN comments LIKE '%TOMORROW%' OR comments LIKE '%TO BE%' OR comments LIKE '%TODAY%' OR comments LIKE '%ON MON%' OR
         comments LIKE '%RE%TRANS%' THEN 'Cancel Transmit: Delayed / Retransmitted'
    WHEN comments LIKE '%BANK%' THEN 'Cancel Transmit: Bank Issue'
    ELSE 'Cancel Transmit: Others' END AS TRIMMED_COMMENT
  FROM os_data.transaction_event
  WHERE status_fk = 1
        AND comments LIKE '%CANCEL%'

UNION ALL

--API Issues
  SELECT
    transaction_fk,
    timestamp                          AS event_dt,
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




select count(*) from (

select status_fk, case
                  when TRIMMED_COMMENT like '%FTP%' then 'FTP Issue'
  when TRIMMED_COMMENT like '%MOBILE NUMBER VALIDATION%' OR TRIMMED_COMMENT like '%MOBILE%VALIDATION%' then 'Mobile Number Issue'
                  when TRIMMED_COMMENT like '%CUSTOMER%REQUEST%' OR TRIMMED_COMMENT like '%RETURNED: AS PER%MAIL%' then 'Customer/Mail Request'
  when TRIMMED_COMMENT like '%KYC%' then 'KYC Failed'
    when TRIMMED_COMMENT like '%DUPLICATE%' then 'Duplicate'
                  when TRIMMED_COMMENT like '%SECURE%SSL%'  or TRIMMED_COMMENT like '%TCP ERROR%'  or TRIMMED_COMMENT like '%MAX WAIT TIME%' or TRIMMED_COMMENT like '%MAX NUMBER OF ATTEMPTS WAS REACHED%'  or TRIMMED_COMMENT like '%TOO MANY CONNECTION%'  or TRIMMED_COMMENT like '%DESTINATION SYSTEM ROUTE ERROR%'  or TRIMMED_COMMENT like '%JDBC%CONNECTION%' or TRIMMED_COMMENT like '%TIMEOUT%' or TRIMMED_COMMENT like '%UNABLE%TO%CONNECT%' or TRIMMED_COMMENT like '%CONNECTIONERROR%' or TRIMMED_COMMENT like '%BINDING%' or TRIMMED_COMMENT like '%MAXIMUM NUMBER OF ATTEMPTS%' or TRIMMED_COMMENT like '%NO ENDPOINT%' OR TRIMMED_COMMENT like '%CONNECTION REFUSED%' then 'Timeout / Server Offline'
                  when TRIMMED_COMMENT like '%SUBSCRIBER%NOT FOUND%' OR  TRIMMED_COMMENT like '%ACCOUNT%NOT FOUND%' OR TRIMMED_COMMENT like '%ACCOUNT%NOT VALID%' OR   TRIMMED_COMMENT like '%PAYER_NOT_FOUND%' OR TRIMMED_COMMENT LIKE '%UNKNOWN RECEIVER%' OR TRIMMED_COMMENT LIKE '%UNABLETOCREDITDESTINATIONACCOUNT%' OR TRIMMED_COMMENT LIKE '%UNABLE TO FIND A RECEIVER%' OR TRIMMED_COMMENT LIKE '%NO ACCOUNT%' OR TRIMMED_COMMENT LIKE '%ACCOUNT%RANGE ERROR%' or  TRIMMED_COMMENT like '%BARRED%' or TRIMMED_COMMENT like '%INCORRECT%' or TRIMMED_COMMENT like '%INVAL%' or TRIMMED_COMMENT like '%blocked%' or TRIMMED_COMMENT like '%FROZEN%' or UPPER(TRIMMED_COMMENT) like '%CLOSED%' or UPPER(TRIMMED_COMMENT) like '%PAYEE_NOT_FOUND%' then 'Invalid or Closed Account'
                  when TRIMMED_COMMENT LIKE 'AOC SUCCEED, BUT REMITTANCE FAILED. MAXIMUM%' or  TRIMMED_COMMENT like '%NO FUNDS%' OR TRIMMED_COMMENT like '%LIMIT%' or  TRIMMED_COMMENT like '%INSUFFI%' or TRIMMED_COMMENT like '%LIMIT EXCEEDED%' or TRIMMED_COMMENT like '%MINIMUMAMOUNT%' or TRIMMED_COMMENT like '%BELOW MINIMUM%' or TRIMMED_COMMENT like '%MIN%CASH%' or TRIMMED_COMMENT like '%MAX%AMOUNT%' or TRIMMED_COMMENT like '%MAX%TOPUP%' then 'Insufficient Funds / Limit Exceeded (MAX/MIN)'
                  when TRIMMED_COMMENT like '%CODE%REQUIRED%' OR TRIMMED_COMMENT like '%missing%' OR TRIMMED_COMMENT like '%NO.%REQUIRED%' OR TRIMMED_COMMENT like '%BENEF%REQUIRED%'  OR TRIMMED_COMMENT like '%ACCOUNT%REQUIRED%'  OR TRIMMED_COMMENT like '%NAME%REQUIRED%' OR TRIMMED_COMMENT like '%TAXPAYER%REQUIRED%' OR TRIMMED_COMMENT like '%BRANCH%REQUIRED%' OR TRIMMED_COMMENT like '%RECIPIENT%REQUIRED%' OR TRIMMED_COMMENT like '%MISSING%ADDRESS%'  OR TRIMMED_COMMENT like '%MISSING%IDEN%' OR TRIMMED_COMMENT like '%MISSING%SENDER%' then 'More Info Required'
                  when TRIMMED_COMMENT like '%PIN%REQUIRED%' OR TRIMMED_COMMENT like '%PIN%tries%' then 'PIN Required'
                  when TRIMMED_COMMENT like '%GETSTATUS%FAILED (%)%' OR  TRIMMED_COMMENT like '%GETTRANSACTIONSTATUS. RESPONSECODE%' then 'Failed to get transaction status'
                  when TRIMMED_COMMENT like '%TOO SHORT%' OR  TRIMMED_COMMENT like '%TOO LONG%' OR  TRIMMED_COMMENT like '%PATTERN%' OR TRIMMED_COMMENT like '%OUTSIDE THE BOUNDS OF THE ARRAY.%'  OR TRIMMED_COMMENT like '%LENGTH%' OR TRIMMED_COMMENT like '%CHARACTER%' OR TRIMMED_COMMENT like '%MAX%LENGTH%' then 'Data format issue'

  when  TRIMMED_COMMENT LIKE '%DOMESTIC%' or TRIMMED_COMMENT like '%DONATION%' OR TRIMMED_COMMENT like '%INSTITUTION%' OR TRIMMED_COMMENT like '%SOCIETY%' OR TRIMMED_COMMENT like '%ASSOCIATION%' OR TRIMMED_COMMENT like '%CHARITY%' OR TRIMMED_COMMENT like '%FOUNDATION%' OR TRIMMED_COMMENT like '%FUND COMPANY%' then 'Insufficient Funds'

when TRIMMED_COMMENT like '%REJECTED BY%BANK%' OR TRIMMED_COMMENT like '%RETURNED BY BANK%' OR TRIMMED_COMMENT like '%AS%PER%EXCHANGE%HOUSE%REJ%'  OR TRIMMED_COMMENT like '%RETURNED%AS PER%BANK%'   OR TRIMMED_COMMENT like '%EXCHANGE HOUSE REJECTED%'  THEN 'Rejected/Returned by Bank'
    when TRIMMED_COMMENT like '%CANCELLATION REQUESTED BY WORLDREMIT/SENDER%' then 'Cancelled by WR'


                  --Lower priority, capturing some very generic stuff
                  when TRIMMED_COMMENT like '%SERVER%ERROR%'  THEN 'Timeout / Server Offline'
                  when TRIMMED_COMMENT LIKE '%RETURNED%ACCOUNT%NOT%' then 'Invalid or Closed Account'

                  WHEN   TRIMMED_COMMENT LIKE '%START FOR API  AND RETURNED THE CODE: 10001%' OR TRIMMED_COMMENT LIKE '%(ERRORCODE%' OR TRIMMED_COMMENT LIKE '%VO WREMIT%'
                        OR TRIMMED_COMMENT LIKE '%GETTRANSACTIONSTATUS RETURNED: 1 - COULD NOT DETERMINE%'
                        OR TRIMMED_COMMENT LIKE '%UNKNOWN%ERROR%'
                      OR TRIMMED_COMMENT LIKE '%UNEXPECTED%ERROR%'
                    OR TRIMMED_COMMENT LIKE '%EXECUTION%RESET%'
                    OR TRIMMED_COMMENT LIKE '%UNABLE TO PROCESS%'
                    OR TRIMMED_COMMENT LIKE '%OBJECT REFERENCE NOT SET TO AN INSTANCE OF AN OBJECT%'
  OR TRIMMED_COMMENT LIKE '%START 28-%'
  OR TRIMMED_COMMENT LIKE '%CANCELLED BY API%'




                    THEN 'Generic API Error'

 WHEN   TRIMMED_COMMENT LIKE '%REVERSED%' OR  TRIMMED_COMMENT LIKE '%RETURNED%' OR  TRIMMED_COMMENT LIKE '%REJECTED%' then 'Other Reject/Return/Reverse'

                  else 'Others'

                    /*REGEXP_REPLACE(TRIMMED_COMMENT,
           '((#|CODE:|REF:)(\S)*)',
           '', 'g')*/


                      end as comm

  , count(*), max(transaction_fk) from TMP_API_fail_comments
  where status_fk = 34

  group by status_fk, comm

) x



SELECT TRIMMED_COMMENT, COUNT(*)
from TMP_API_fail_comments
    where status_fk = 1

GROUP BY TRIMMED_COMMENT


select * from os_data.transaction_event where transaction_fk = 6725865


/*====================================================================================================================*/
/*==                                    Repeat Sales and Inactivity                                           ==*/
/*====================================================================================================================*/
--Duration: 36m to 1h

DROP TABLE IF EXISTS TMP_transactions;

CREATE TEMPORARY TABLE TMP_transactions AS
  SELECT
    A.transaction_id,
    sum(
        CASE WHEN B.created_at BETWEEN A.created_at + INTERVAL '1 day' AND A.created_at + INTERVAL '3 month'
          THEN 1 ELSE 0 END)                         AS NUM_tansactions_3M,
    count(DISTINCT
        CASE WHEN B.created_at BETWEEN date_trunc('month', A.created_at) + INTERVAL '1 month' AND
        date_trunc('month', A.created_at) + INTERVAL '3 month'
          THEN EXTRACT(MONTH FROM B.created_at) END) AS NUM_months_with_transactions_3M
  FROM TIME_LAPSED_AUTH A
    LEFT JOIN TIME_LAPSED_AUTH B ON a.user_fk = b.user_fk
  WHERE a.created_at <= get_last_quarter_end_date(get_last_quarter_end_date(now()))
  GROUP BY A.transaction_id;
--We filter by end of the quarter before the previous, to remove info that is not mature enough.
--e.g. for the latest quarter we cannot calculate quarterly churn or repeat sale, so we only want to give the information from the quarter before the previous and older.

create index on TMP_transactions(transaction_id);

ALTER TABLE TIME_LAPSED_AUTH
  ADD NUM_tansactions_3M INTEGER,
  ADD NUM_months_with_transactions_3M INTEGER;


UPDATE TIME_LAPSED_AUTH A
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
  FROM TIME_LAPSED_AUTH A
    LEFT JOIN os_data.sf_case_all B ON a.user_fk = b.user_fk and B.createddate >= A.created_at

  GROUP BY A.transaction_id;
--We filter by end of the quarter before the previous, to remove info that is not mature enough.
--e.g. for the latest quarter we cannot calculate quarterly churn or repeat sale, so we only want to give the information from the quarter before the previous and older.

create index on TMP_transactions(transaction_id);

ALTER TABLE TIME_LAPSED_AUTH
  ADD num_cases INTEGER;


UPDATE TIME_LAPSED_AUTH A
SET num_cases              = b.num_cases
FROM TMP_transactions B
WHERE a.transaction_id = b.transaction_id;



/*====================================================================================================================*/
/*==                                    Summarized Information for Output                                           ==*/
/*====================================================================================================================*/

/* Summarize information */

create table RQ2010705_TRANSACTION_TIME_ANALYSIS_PIVOT as

SELECT
  sender_country,
  receiver_country,
  receive_method_name,
  correspondent_name,
  /*payment_method_name, card_type,*/
  FL_PRE_AUTH,
  FL_POST_AUTH,
  is_first_transaction                              AS FL_first_transaction,

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

  count(*)                                          AS transactions,
  sum(FL_PRE_AUTH)                                  AS transactions_preauth,
  sum(FL_POST_AUTH)                                 AS transactions_postauth,
  sum(time_lapse_creation_to_preauth)               AS time_lapse_creation_to_preauth,
  sum(time_lapse_preauth_to_authorized)             AS time_lapse_preauth_to_authorized,
  sum(time_lapse_creation_to_authorized)            AS time_lapse_creation_to_authorized,
  sum(time_lapse_auth_to_postauth)                  AS time_lapse_auth_to_postauth,
  sum(time_lapse_postauth_to_paid)                  AS time_lapse_postauth_to_paid,
  sum(time_lapse_authorized_to_paid)                AS time_lapse_authorized_to_paid,
  sum(time_lapse_creation_to_paid)                  AS time_lapse_creation_to_paid,


  sum(NUM_tansactions_3M) as NUM_tansactions_3M,
  sum(NUM_months_with_transactions_3M) as NUM_months_with_transactions_3M



FROM TIME_LAPSED_AUTH
GROUP BY sender_country
  , receiver_country
  , receive_method_name
  , correspondent_name
  /*, payment_method_name, card_type,*/
  , FL_PRE_AUTH
  , FL_POST_AUTH
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
  , transaction_friction_postauth;






/* Decile Summaries */

select receive_method_name,quartile_time_lapse_creation_to_authorized, count(*), avg(time_lapse_creation_to_authorized), max(time_lapse_creation_to_authorized)
from TIME_LAPSED_AUTH
group by receive_method_name,quartile_time_lapse_creation_to_authorized;

select receive_method_name,quartile_time_lapse_authorized_to_paid, count(*), avg(time_lapse_authorized_to_paid), max(time_lapse_authorized_to_paid)
from TIME_LAPSED_AUTH
group by receive_method_name,quartile_time_lapse_authorized_to_paid;


/*

  SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position, routines.specific_schema
FROM information_schema.routines
    JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
WHERE routines.routine_name like '%get_quarter%'
ORDER BY routines.routine_name, parameters.ordinal_position;
 */



select *
from TIME_LAPSED_AUTH
where FL_PRE_AUTH = 0
and TIME_LAPSED_AUTH.transaction_friction_preauth is null



select *
from cd_data.transaction_lapsed_pre_post_auth
 where transaction_fk in (
6455756,
6455825,
6456341,
6456743,
6457437,
6457608
);


transaction_fk,received_time,authorized_time,first_pre_auth_event,last_pre_auth_event
6457437,2016-01-01 10:29:49.000000,2016-01-01 13:53:56.000000,2016-01-01 10:30:28.000000,2016-01-01 10:30:28.000000
6457608,2016-01-01 10:54:19.000000,2016-01-01 11:56:43.000000,2016-01-01 10:54:56.000000,2016-01-01 10:54:56.000000
6455825,2016-01-01 05:06:10.000000,2016-01-01 08:33:35.000000,2016-01-01 05:06:48.000000,2016-01-01 05:06:48.000000
6455756,2016-01-01 04:43:35.000000,2016-01-01 08:31:57.000000,2016-01-01 04:44:14.000000,2016-01-01 04:44:14.000000
6456743,2016-01-01 08:48:20.000000,2016-01-01 09:42:16.000000,2016-01-01 08:48:57.000000,2016-01-01 08:48:57.000000
6456341,2016-01-01 07:20:50.000000,2016-01-01 08:34:57.000000,2016-01-01 07:21:30.000000,2016-01-01 07:21:30.000000



select * from os_data.transaction_event where transaction_fk in (
6455756,
6455825,
6456341,
6456743,
6457437,
6457608
)